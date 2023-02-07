package me.iban.enjinprofilescrape

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.sqlite.SQLiteDataSource
import java.sql.Connection
import javax.sql.DataSource

object Database {

    lateinit var ds: DataSource

    private const val TABLE_NAME = "profiles"
    private const val CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS $TABLE_NAME (
            ID int,
            Result varchar(255),
            ProfileViews int,
            DisplayName varchar(255),
            Quote varchar(255),
            Bio text,
            CustomURL varchar(255),
            Friends int,
            LastSeen bigint,
            JoinDate bigint,
            PRIMARY KEY (ID)
        )
    """

    private const val HAS_ENTRY = """
        SELECT * FROM $TABLE_NAME
        WHERE ID = ?
    """

    private const val INSERT_ENTRY = """
        INSERT INTO $TABLE_NAME
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    private const val UPDATE_ENTRY = """
        UPDATE $TABLE_NAME
        SET Result = ?, ProfileViews = ?, DisplayName = ?, Quote = ?, Bio = ?, CustomURL = ?, Friends = ?, LastSeen = ?, JoinDate = ?
        WHERE ID = ?
    """

    private const val LATEST_PROFILE = """
        SELECT MAX(ID)
        FROM $TABLE_NAME;
    """

    fun init(host: String? = null, port: Int? = null, user: String? = null, pass: String? = null) {
        if (host != null && port != null && user != null && pass != null) {
            println("Information provided, using mySQL")
            Class.forName("com.mysql.cj.jdbc.Driver")
            val config = HikariConfig()
            config.apply {
                jdbcUrl = "jdbc:mysql://$host:$port/enjin?characterEncoding=latin1"
                username = user
                password = pass
                maximumPoolSize = 16
                leakDetectionThreshold = 60 * 1000

                addDataSourceProperty("tcpKeepAlive", true)
                addDataSourceProperty("cachePrepStmts", true)
            }
            ds = HikariDataSource(config)
        } else {
            ds = SQLiteDataSource().apply {
                url = "jdbc:sqlite:enjin.profiles"
            }
        }
        doSQL {
            prepareStatement(CREATE_TABLE).executeUpdate()
        }
    }

    fun isInitialized(): Boolean {
        return this::ds.isInitialized
    }

    fun hasProfile(profileID: Int): Boolean {
        return doSQL {
            prepareStatement(HAS_ENTRY).apply {
                setInt(1, profileID)
            }.executeQuery().use { it.isBeforeFirst }
        }
    }

    fun insertProfileOrUpdateIfExists(profile: EnjinProfile) {
        doSQL {
            if (!hasProfile(profile.id)) {
                prepareStatement(INSERT_ENTRY).apply {
                    setInt(1, profile.id)
                    setString(2, profile.result.name)
                    setInt(3, profile.profileViews)
                    setString(4, profile.displayName)
                    setString(5, profile.quote)
                    setString(6, profile.bio)
                    setString(7, profile.customUrl)
                    setInt(8, profile.friends)
                    setLong(9, profile.lastSeen)
                    setLong(10, profile.joinDate)
                }.use { it.executeUpdate() }
                return@doSQL
            }
            prepareStatement(UPDATE_ENTRY).apply {
                setString(1, profile.result.name)
                setInt(2, profile.profileViews)
                setString(3, profile.displayName)
                setString(4, profile.quote)
                setString(5, profile.bio)
                setString(6, profile.customUrl)
                setInt(7, profile.friends)
                setLong(8, profile.lastSeen)
                setLong(9, profile.joinDate)
                setInt(10, profile.id)
            }.use { it.executeUpdate() }
        }
    }

    fun getLatestProfileID(): Int {
        return doSQL {
            val query = prepareStatement(LATEST_PROFILE).executeQuery()
            return query.use {
                if (it.next()) {
                    it.getInt(1)
                } else {
                    0
                }
            }
        }
    }

}

inline fun <R> doSQL(block: Connection.() -> R): R {
    if (!Database.isInitialized()) {
        throw Exception("Database is not initalized")
    }
    return Database.ds.connection.use(block)
}