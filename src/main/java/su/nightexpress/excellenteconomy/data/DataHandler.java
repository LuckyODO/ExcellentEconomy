package su.nightexpress.excellenteconomy.data;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.jspecify.annotations.NonNull;

import su.nightexpress.excellenteconomy.EconomyPlugin;
import su.nightexpress.excellenteconomy.api.currency.ExcellentCurrency;
import su.nightexpress.excellenteconomy.user.CoinsUser;
import su.nightexpress.excellenteconomy.user.data.CurrencySettings;
import su.nightexpress.excellenteconomy.user.data.CurrencySettingsSerializer;
import su.nightexpress.nightcore.db.AbstractDatabaseManager;
import su.nightexpress.nightcore.db.column.Column;
import su.nightexpress.nightcore.db.config.DatabaseType;
import su.nightexpress.nightcore.db.statement.SQLStatements;
import su.nightexpress.nightcore.db.statement.condition.Operator;
import su.nightexpress.nightcore.db.statement.condition.Wheres;
import su.nightexpress.nightcore.db.statement.template.InsertStatement;
import su.nightexpress.nightcore.db.statement.template.SelectStatement;
import su.nightexpress.nightcore.db.statement.template.UpdateStatement;
import su.nightexpress.nightcore.db.table.Table;
import su.nightexpress.nightcore.user.data.UserDataSchema;
import su.nightexpress.nightcore.util.TimeUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class DataHandler extends AbstractDatabaseManager<EconomyPlugin> implements UserDataSchema<CoinsUser> {

    public static final Gson GSON = new GsonBuilder().setPrettyPrinting()
        .registerTypeAdapter(CurrencySettings.class, new CurrencySettingsSerializer())
        .create();

    private static final String UUID_INDEX = "idx_execo_users_uuid";
    private static final String NAME_INDEX = "idx_execo_users_name";

    private final Table usersTable;

    private boolean synchronizationActive; // A little helper to pause synchronization during operations disable

    public DataHandler(@NonNull EconomyPlugin plugin) {
        super(plugin);
        this.setSynchronizationActive(true);

        this.usersTable = Table.builder(this.getTablePrefix() + "_users")
            .withColumn(DataColumns.ID)
            .withColumn(DataColumns.USER_UUID)
            .withColumn(DataColumns.USER_NAME)
            .withColumn(DataColumns.USER_LAST_SEEN)
            .withColumn(DataColumns.USER_SETTINGS)
            .withColumn(DataColumns.USER_HIDE_FROM_TOPS)
            .build();
    }

    @Override
    protected void onInitialize() {
        this.createTable(this.usersTable);

        this.dropColumn(this.usersTable, "dateCreated");
        this.dropColumn(this.usersTable, "last_online");
    }

    @Override
    protected void onClose() {
        DataColumns.clearCache();
    }

    @Override
    public void onPurge() {
        int period = this.config.getPurgePeriod();
        long deadline = TimeUtil.toEpochMillis(TimeUtil.getCurrentDateTime().minusDays(period));

        this.delete(this.usersTable, Wheres.where(DataColumns.USER_LAST_SEEN, Operator.SMALLER, o -> deadline));
    }

    @Override
    public void onSynchronize() {
        // Do not synchronize data if operations are disabled to prevent data loss/clash.
        if (!this.synchronizationActive) return;

        this.synchronizer.syncAll();
    }

    public void onCurrencyRegister(@NonNull ExcellentCurrency currency) {
        this.addCurrencyColumn(currency);
    }

    public void onCurrencyUnload(@NonNull ExcellentCurrency currency) {
        DataColumns.uncacheCurrency(currency);
    }

    public void setSynchronizationActive(boolean synchronizationActive) {
        this.synchronizationActive = synchronizationActive;
    }

    public void addCurrencyColumn(@NonNull ExcellentCurrency currency) {
        this.addColumn(this.usersTable, DataColumns.forCurrency(currency));
    }

    public boolean isAtomicBalances() {
        return this.databaseType == DatabaseType.MYSQL;
    }

    public void ensureUserIntegrity(@NonNull Collection<ExcellentCurrency> currencies) {
        if (this.databaseType != DatabaseType.MYSQL) return;

        this.mergeDuplicateUsers(currencies);
        this.ensureIndex(UUID_INDEX, true, DataColumns.USER_UUID);
        this.ensureIndex(NAME_INDEX, false, DataColumns.USER_NAME);
    }

    private void ensureIndex(@NonNull String indexName, boolean unique, @NonNull Column<?> column) {
        if (SQLStatements.hasIndex(this.connector, this.databaseType, this.usersTable.getName(), indexName)) return;

        String sql = "CREATE " + (unique ? "UNIQUE " : "") + "INDEX " + quote(indexName) + " ON " +
            quote(this.usersTable.getName()) + " (" + quote(column.getName()) + ")";
        SQLStatements.executeUpdate(this.connector, sql);
    }

    private void mergeDuplicateUsers(@NonNull Collection<ExcellentCurrency> currencies) {
        String sql = "SELECT " + quote(DataColumns.USER_UUID.getName()) + " FROM " + quote(this.usersTable.getName()) +
            " GROUP BY " + quote(DataColumns.USER_UUID.getName()) + " HAVING COUNT(*) > 1";

        try (Connection connection = this.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {

            while (resultSet.next()) {
                this.mergeDuplicateUser(resultSet.getString(DataColumns.USER_UUID.getName()), currencies);
            }
        }
        catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    private void mergeDuplicateUser(@NonNull String uuid, @NonNull Collection<ExcellentCurrency> currencies) {
        try (Connection connection = this.getConnection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            try {
                MergeState state = this.readDuplicateState(connection, uuid, currencies);
                if (state == null || state.duplicates() <= 0) {
                    connection.rollback();
                    return;
                }

                this.updateMergedUser(connection, state, currencies);
                this.deleteDuplicateRows(connection, uuid, state.keepId());
                connection.commit();

                this.plugin.warn("Merged " + state.duplicates() + " duplicate economy user row(s) for UUID " + uuid + ".");
            }
            catch (SQLException exception) {
                connection.rollback();
                throw exception;
            }
            finally {
                connection.setAutoCommit(autoCommit);
            }
        }
        catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    private MergeState readDuplicateState(@NonNull Connection connection,
                                          @NonNull String uuid,
                                          @NonNull Collection<ExcellentCurrency> currencies) throws SQLException {
        String currencyColumns = currencies.stream()
            .map(currency -> quote(DataColumns.forCurrency(currency).getName()))
            .collect(Collectors.joining(", "));
        String currencySelect = currencyColumns.isEmpty() ? "" : ", " + currencyColumns;

        String sql = "SELECT " + quote(DataColumns.ID.getName()) + ", " + quote(DataColumns.USER_NAME.getName()) +
            ", " + quote(DataColumns.USER_SETTINGS.getName()) + ", " + quote(DataColumns.USER_LAST_SEEN.getName()) +
            ", " + quote(DataColumns.USER_HIDE_FROM_TOPS.getName()) + currencySelect +
            " FROM " + quote(this.usersTable.getName()) +
            " WHERE " + quote(DataColumns.USER_UUID.getName()) + " = ?" +
            " ORDER BY " + quote(DataColumns.USER_LAST_SEEN.getName()) + " DESC, " + quote(DataColumns.ID.getName()) + " DESC";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, uuid);

            try (ResultSet resultSet = statement.executeQuery()) {
                Integer keepId = null;
                String name = "";
                String settings = "{}";
                long lastSeen = 0L;
                boolean hiddenFromTops = false;
                int rows = 0;
                Map<String, Double> balances = new LinkedHashMap<>();

                while (resultSet.next()) {
                    rows++;

                    if (keepId == null) {
                        keepId = resultSet.getInt(DataColumns.ID.getName());
                        name = resultSet.getString(DataColumns.USER_NAME.getName());
                        settings = resultSet.getString(DataColumns.USER_SETTINGS.getName());
                        lastSeen = resultSet.getLong(DataColumns.USER_LAST_SEEN.getName());
                        hiddenFromTops = resultSet.getBoolean(DataColumns.USER_HIDE_FROM_TOPS.getName());
                    }

                    for (ExcellentCurrency currency : currencies) {
                        String column = DataColumns.forCurrency(currency).getName();
                        double value = resultSet.getDouble(column);
                        if (resultSet.wasNull()) continue;

                        balances.merge(column, value, Math::max);
                    }
                }

                if (keepId == null) return null;

                return new MergeState(keepId, Math.max(0, rows - 1), name, settings == null ? "{}" : settings,
                    lastSeen, hiddenFromTops, balances);
            }
        }
    }

    private void updateMergedUser(@NonNull Connection connection,
                                  @NonNull MergeState state,
                                  @NonNull Collection<ExcellentCurrency> currencies) throws SQLException {
        String balanceAssignments = currencies.stream()
            .map(currency -> quote(DataColumns.forCurrency(currency).getName()) + " = ?")
            .collect(Collectors.joining(", "));
        String balancesSql = balanceAssignments.isEmpty() ? "" : ", " + balanceAssignments;

        String sql = "UPDATE " + quote(this.usersTable.getName()) + " SET " +
            quote(DataColumns.USER_NAME.getName()) + " = ?, " +
            quote(DataColumns.USER_SETTINGS.getName()) + " = ?, " +
            quote(DataColumns.USER_LAST_SEEN.getName()) + " = ?, " +
            quote(DataColumns.USER_HIDE_FROM_TOPS.getName()) + " = ?" + balancesSql +
            " WHERE " + quote(DataColumns.ID.getName()) + " = ?";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            statement.setString(index++, state.name());
            statement.setString(index++, state.settings());
            statement.setLong(index++, state.lastSeen());
            statement.setBoolean(index++, state.hiddenFromTops());

            for (ExcellentCurrency currency : currencies) {
                String column = DataColumns.forCurrency(currency).getName();
                statement.setDouble(index++, state.balances().getOrDefault(column, currency.getStartValue()));
            }

            statement.setInt(index, state.keepId());
            statement.executeUpdate();
        }
    }

    private void deleteDuplicateRows(@NonNull Connection connection, @NonNull String uuid, int keepId) throws SQLException {
        String sql = "DELETE FROM " + quote(this.usersTable.getName()) +
            " WHERE " + quote(DataColumns.USER_UUID.getName()) + " = ?" +
            " AND " + quote(DataColumns.ID.getName()) + " <> ?";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, uuid);
            statement.setInt(2, keepId);
            statement.executeUpdate();
        }
    }

    @NonNull
    public OptionalDouble fetchBalance(@NonNull UUID uuid, @NonNull ExcellentCurrency currency) {
        if (!this.isAtomicBalances()) return OptionalDouble.empty();

        try (Connection connection = this.getConnection()) {
            return this.selectBalance(connection, uuid, currency);
        }
        catch (SQLException exception) {
            exception.printStackTrace();
            return OptionalDouble.empty();
        }
    }

    @NonNull
    public BalanceResult addBalance(@NonNull CoinsUser user, @NonNull ExcellentCurrency currency, double amount) {
        return this.updateBalanceDelta(user.getId(), currency, Math.abs(amount), user.getBalance(currency));
    }

    @NonNull
    public BalanceResult removeBalance(@NonNull CoinsUser user, @NonNull ExcellentCurrency currency, double amount) {
        return this.updateBalanceDelta(user.getId(), currency, -Math.abs(amount), user.getBalance(currency));
    }

    @NonNull
    public BalanceResult withdrawBalance(@NonNull CoinsUser user, @NonNull ExcellentCurrency currency, double amount) {
        double positiveAmount = Math.abs(amount);
        String column = quote(DataColumns.forCurrency(currency).getName());
        String sql = "UPDATE " + quote(this.usersTable.getName()) +
            " SET " + column + " = " + column + " - ?" +
            " WHERE " + quote(DataColumns.USER_UUID.getName()) + " = ? AND " + column + " >= ?";

        try (Connection connection = this.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setDouble(1, positiveAmount);
            statement.setString(2, user.getId().toString());
            statement.setDouble(3, positiveAmount);

            if (statement.executeUpdate() <= 0) {
                OptionalDouble balance = this.selectBalance(connection, user.getId(), currency);
                return balance.isPresent() ? BalanceResult.insufficient(balance.getAsDouble()) : BalanceResult.missing();
            }

            OptionalDouble balance = this.selectBalance(connection, user.getId(), currency);
            return balance.isPresent() ? BalanceResult.success(balance.getAsDouble()) : BalanceResult.missing();
        }
        catch (SQLException exception) {
            exception.printStackTrace();
            return BalanceResult.failure(user.getBalance(currency));
        }
    }

    @NonNull
    public BalanceResult setBalance(@NonNull CoinsUser user, @NonNull ExcellentCurrency currency, double amount) {
        double balance = currency.floorAndLimit(amount);
        String column = quote(DataColumns.forCurrency(currency).getName());
        String sql = "UPDATE " + quote(this.usersTable.getName()) +
            " SET " + column + " = ?" +
            " WHERE " + quote(DataColumns.USER_UUID.getName()) + " = ?";

        try (Connection connection = this.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setDouble(1, balance);
            statement.setString(2, user.getId().toString());
            statement.executeUpdate();

            OptionalDouble fetched = this.selectBalance(connection, user.getId(), currency);
            return fetched.isPresent() ? BalanceResult.success(fetched.getAsDouble()) : BalanceResult.missing();
        }
        catch (SQLException exception) {
            exception.printStackTrace();
            return BalanceResult.failure(user.getBalance(currency));
        }
    }

    @NonNull
    public TransferResult transfer(@NonNull CoinsUser source,
                                   @NonNull CoinsUser target,
                                   @NonNull ExcellentCurrency currency,
                                   double amount) {
        double positiveAmount = Math.abs(amount);
        String column = quote(DataColumns.forCurrency(currency).getName());

        try (Connection connection = this.getConnection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            try {
                String withdrawSql = "UPDATE " + quote(this.usersTable.getName()) +
                    " SET " + column + " = " + column + " - ?" +
                    " WHERE " + quote(DataColumns.USER_UUID.getName()) + " = ? AND " + column + " >= ?";

                try (PreparedStatement statement = connection.prepareStatement(withdrawSql)) {
                    statement.setDouble(1, positiveAmount);
                    statement.setString(2, source.getId().toString());
                    statement.setDouble(3, positiveAmount);

                    if (statement.executeUpdate() <= 0) {
                        OptionalDouble sourceBalance = this.selectBalance(connection, source.getId(), currency);
                        connection.rollback();
                        connection.setAutoCommit(autoCommit);
                        return sourceBalance.isPresent()
                            ? TransferResult.insufficient(sourceBalance.getAsDouble(), target.getBalance(currency))
                            : TransferResult.missing(source.getBalance(currency), target.getBalance(currency));
                    }
                }

                String depositSql = "UPDATE " + quote(this.usersTable.getName()) +
                    " SET " + column + " = GREATEST(0, " + column + " + ?)" +
                    " WHERE " + quote(DataColumns.USER_UUID.getName()) + " = ?";

                try (PreparedStatement statement = connection.prepareStatement(depositSql)) {
                    statement.setDouble(1, positiveAmount);
                    statement.setString(2, target.getId().toString());

                    if (statement.executeUpdate() <= 0) {
                        connection.rollback();
                        connection.setAutoCommit(autoCommit);
                        return TransferResult.missing(source.getBalance(currency), target.getBalance(currency));
                    }
                }

                double sourceBalance = this.selectBalance(connection, source.getId(), currency).orElse(0D);
                double targetBalance = this.selectBalance(connection, target.getId(), currency).orElse(0D);

                connection.commit();
                connection.setAutoCommit(autoCommit);
                return TransferResult.success(sourceBalance, targetBalance);
            }
            catch (SQLException exception) {
                connection.rollback();
                connection.setAutoCommit(autoCommit);
                throw exception;
            }
        }
        catch (SQLException exception) {
            exception.printStackTrace();
            return TransferResult.failure(source.getBalance(currency), target.getBalance(currency));
        }
    }

    @NonNull
    public TransferResult exchange(@NonNull CoinsUser user,
                                   @NonNull ExcellentCurrency sourceCurrency,
                                   @NonNull ExcellentCurrency targetCurrency,
                                   double amount,
                                   double result) {
        String sourceColumn = quote(DataColumns.forCurrency(sourceCurrency).getName());
        String targetColumn = quote(DataColumns.forCurrency(targetCurrency).getName());
        String limitWhere = targetCurrency.isLimited() ? " AND " + targetColumn + " + ? <= ?" : "";

        String sql = "UPDATE " + quote(this.usersTable.getName()) +
            " SET " + sourceColumn + " = " + sourceColumn + " - ?, " +
            targetColumn + " = " + targetColumn + " + ?" +
            " WHERE " + quote(DataColumns.USER_UUID.getName()) + " = ? AND " + sourceColumn + " >= ?" + limitWhere;

        try (Connection connection = this.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            int index = 1;
            statement.setDouble(index++, amount);
            statement.setDouble(index++, result);
            statement.setString(index++, user.getId().toString());
            statement.setDouble(index++, amount);

            if (targetCurrency.isLimited()) {
                statement.setDouble(index++, result);
                statement.setDouble(index, targetCurrency.getMaxValue());
            }

            if (statement.executeUpdate() <= 0) {
                OptionalDouble sourceBalance = this.selectBalance(connection, user.getId(), sourceCurrency);
                OptionalDouble targetBalance = this.selectBalance(connection, user.getId(), targetCurrency);

                if (sourceBalance.isEmpty() || targetBalance.isEmpty()) {
                    return TransferResult.missing(user.getBalance(sourceCurrency), user.getBalance(targetCurrency));
                }
                if (sourceBalance.getAsDouble() < amount) {
                    return TransferResult.insufficient(sourceBalance.getAsDouble(), targetBalance.getAsDouble());
                }
                if (targetCurrency.isLimited() && targetBalance.getAsDouble() + result > targetCurrency.getMaxValue()) {
                    return TransferResult.limit(sourceBalance.getAsDouble(), targetBalance.getAsDouble());
                }
                return TransferResult.failure(sourceBalance.getAsDouble(), targetBalance.getAsDouble());
            }

            double sourceBalance = this.selectBalance(connection, user.getId(), sourceCurrency).orElse(0D);
            double targetBalance = this.selectBalance(connection, user.getId(), targetCurrency).orElse(0D);
            return TransferResult.success(sourceBalance, targetBalance);
        }
        catch (SQLException exception) {
            exception.printStackTrace();
            return TransferResult.failure(user.getBalance(sourceCurrency), user.getBalance(targetCurrency));
        }
    }

    @NonNull
    private BalanceResult updateBalanceDelta(@NonNull UUID uuid,
                                             @NonNull ExcellentCurrency currency,
                                             double delta,
                                             double fallback) {
        String column = quote(DataColumns.forCurrency(currency).getName());
        String operator = delta >= 0D ? "+" : "-";
        String sql = "UPDATE " + quote(this.usersTable.getName()) +
            " SET " + column + " = GREATEST(0, " + column + " " + operator + " ?)" +
            " WHERE " + quote(DataColumns.USER_UUID.getName()) + " = ?";

        try (Connection connection = this.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setDouble(1, Math.abs(delta));
            statement.setString(2, uuid.toString());
            statement.executeUpdate();

            OptionalDouble balance = this.selectBalance(connection, uuid, currency);
            return balance.isPresent() ? BalanceResult.success(balance.getAsDouble()) : BalanceResult.missing();
        }
        catch (SQLException exception) {
            exception.printStackTrace();
            return BalanceResult.failure(fallback);
        }
    }

    @NonNull
    private OptionalDouble selectBalance(@NonNull Connection connection,
                                         @NonNull UUID uuid,
                                         @NonNull ExcellentCurrency currency) throws SQLException {
        String sql = "SELECT " + quote(DataColumns.forCurrency(currency).getName()) +
            " FROM " + quote(this.usersTable.getName()) +
            " WHERE " + quote(DataColumns.USER_UUID.getName()) + " = ? LIMIT 1";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, uuid.toString());

            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) return OptionalDouble.empty();

                return OptionalDouble.of(Math.max(0D, resultSet.getDouble(1)));
            }
        }
    }

    @Override
    @NonNull
    public Table getUsersTable() {
        return this.usersTable;
    }

    @Override
    @NonNull
    public Column<UUID> getUserIdColumn() {
        return DataColumns.USER_UUID;
    }

    @Override
    @NonNull
    public Column<String> getUserNameColumn() {
        return DataColumns.USER_NAME;
    }

    @Override
    @NonNull
    public SelectStatement<CoinsUser> getUserSelectStatement() {
        return DataQueries.userSelect();
    }

    @Override
    @NonNull
    public InsertStatement<CoinsUser> getUserInsertStatement() {
        return DataQueries.userInsert();
    }

    @Override
    @NonNull
    public UpdateStatement<CoinsUser> getUserUpdateStatement() {
        return DataQueries.userUpdate(!this.isAtomicBalances());
    }

    @Override
    @NonNull
    public UpdateStatement<CoinsUser> getUserTinyUpdateStatement() {
        return DataQueries.userTinyUpdate();
    }

    public void resetBalances(@NonNull ExcellentCurrency currency) {
        this.resetBalances(Set.of(currency));
    }

    public void resetBalances(@NonNull Collection<ExcellentCurrency> currencies) {
        UpdateStatement.Builder<Object> builder = UpdateStatement.builder();

        for (ExcellentCurrency currency : currencies) {
            builder.setDouble(DataColumns.forCurrency(currency), o -> currency.getStartValue());
        }

        this.update(this.usersTable, builder.build(), new Object());
    }

    @NonNull
    private static String quote(@NonNull String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private record MergeState(
        int keepId,
        int duplicates,
        @NonNull String name,
        @NonNull String settings,
        long lastSeen,
        boolean hiddenFromTops,
        @NonNull Map<String, Double> balances
    ) {
    }

    public enum BalanceStatus {
        SUCCESS,
        FAILURE,
        INSUFFICIENT_FUNDS,
        LIMIT_EXCEEDED,
        MISSING_ACCOUNT
    }

    public record BalanceResult(@NonNull BalanceStatus status, double balance) {

        public boolean success() {
            return this.status == BalanceStatus.SUCCESS;
        }

        @NonNull
        public static BalanceResult success(double balance) {
            return new BalanceResult(BalanceStatus.SUCCESS, balance);
        }

        @NonNull
        public static BalanceResult failure(double balance) {
            return new BalanceResult(BalanceStatus.FAILURE, balance);
        }

        @NonNull
        public static BalanceResult insufficient(double balance) {
            return new BalanceResult(BalanceStatus.INSUFFICIENT_FUNDS, balance);
        }

        @NonNull
        public static BalanceResult missing() {
            return new BalanceResult(BalanceStatus.MISSING_ACCOUNT, 0D);
        }
    }

    public record TransferResult(@NonNull BalanceStatus status, double sourceBalance, double targetBalance) {

        public boolean success() {
            return this.status == BalanceStatus.SUCCESS;
        }

        @NonNull
        public static TransferResult success(double sourceBalance, double targetBalance) {
            return new TransferResult(BalanceStatus.SUCCESS, sourceBalance, targetBalance);
        }

        @NonNull
        public static TransferResult failure(double sourceBalance, double targetBalance) {
            return new TransferResult(BalanceStatus.FAILURE, sourceBalance, targetBalance);
        }

        @NonNull
        public static TransferResult insufficient(double sourceBalance, double targetBalance) {
            return new TransferResult(BalanceStatus.INSUFFICIENT_FUNDS, sourceBalance, targetBalance);
        }

        @NonNull
        public static TransferResult limit(double sourceBalance, double targetBalance) {
            return new TransferResult(BalanceStatus.LIMIT_EXCEEDED, sourceBalance, targetBalance);
        }

        @NonNull
        public static TransferResult missing(double sourceBalance, double targetBalance) {
            return new TransferResult(BalanceStatus.MISSING_ACCOUNT, sourceBalance, targetBalance);
        }
    }
}
