package me.iban.enjinprofilescrape

import org.sqlite.SQLiteDataSource
import java.sql.Connection

object Database {

    val ds = SQLiteDataSource().apply { url = "jdbc:sqlite:enjin.profiles" }

    private const val TABLE_NAME = "profiles"
    private const val CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS $TABLE_NAME (
            ID int,
            Result varchar(255),
            ProfileViews int,
            DisplayName varchar(255),
            Quote varchar(255),
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    private const val UPDATE_ENTRY = """
        UPDATE $TABLE_NAME
        SET Result = ?, ProfileViews = ?, DisplayName = ?, Quote = ?, CustomURL = ?, Friends = ?, LastSeen = ?, JoinDate = ?
        WHERE ID = ?
    """

    private const val LATEST_PROFILE = """
        SELECT MAX(ID)
        FROM $TABLE_NAME;
    """

    fun init() {
        doSQL {
            prepareStatement(CREATE_TABLE).executeUpdate()
        }
    }

    fun hasProfile(profileID: Int): Boolean {
        return doSQL {
            prepareStatement(HAS_ENTRY).apply {
                setInt(1, profileID)
            }.executeQuery().isBeforeFirst
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
                    setString(6, profile.customUrl)
                    setInt(7, profile.friends)
                    setLong(8, profile.lastSeen)
                    setLong(9, profile.joinDate)
                }.executeUpdate()
                return@doSQL
            }
            prepareStatement(UPDATE_ENTRY).apply {
                setString(1, profile.result.name)
                setInt(2, profile.profileViews)
                setString(3, profile.displayName)
                setString(4, profile.quote)
                setString(5, profile.customUrl)
                setInt(6, profile.friends)
                setLong(7, profile.lastSeen)
                setLong(8, profile.joinDate)
                setInt(9, profile.id)
            }.executeUpdate()
        }
    }

    fun getLatestProfileID(): Int {
        return doSQL {
            val query = prepareStatement(LATEST_PROFILE).executeQuery()
            return if (query.next()) {
                query.getInt(1)
            } else {
                0
            }
        }
    }

}

inline fun <R> doSQL(block: Connection.() -> R): R {
    return Database.ds.connection.use(block)
}