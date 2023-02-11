package me.iban.enjinprofilescrape

import org.jsoup.Jsoup
import org.sqlite.SQLiteDataSource
import java.net.InetSocketAddress
import java.net.Proxy
import java.sql.PreparedStatement
import java.text.NumberFormat
import java.time.*
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAdjusters
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

val dateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("MMM d, yy")
    .withLocale(Locale.getDefault())
    .withZone(ZoneId.systemDefault())

val joinDateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("d MMMM yyyy")
    .withLocale(Locale.getDefault())
    .withZone(ZoneId.systemDefault())

const val newestProfile = 21024144

val lastProfile = AtomicInteger(-1)

// honestly idk if this *has* to be atomic
val lastUploadTime = AtomicLong(-1)
const val DELAY = 1000 * 30L

val queue = ConcurrentLinkedQueue<EnjinProfile>()

var debug = false

fun printlnOnDebug(str: String) {
    if (debug) println(str)
}

fun main(args: Array<String>) {
    // -DstartProfile
    //proxies are in format 127.0.0.1:XXXXX,127.0.0.1:XXXXX,...
    // -Dproxies
    debug = args.contains("debug")
    printlnOnDebug("Running in debug mode!")

    val proxies = System.getProperty("proxies")?.split(",")?.toMutableSet() ?: mutableSetOf()

    // -DproxyRangeHost
    // -DproxyRangePorts - 8888:8950
    val proxyRangeHost = System.getProperty("proxyRangeHost")
    val proxyRangePorts = System.getProperty("proxyRangePorts")
    if (proxyRangeHost != null && proxyRangePorts != null) {
        val split = proxyRangePorts.split(":").mapNotNull { it.toIntOrNull() }
        if (split.size == 2) {
            for (port in split[0]..split[1]) {
                proxies.add("$proxyRangeHost:$port")
            }
        }
    }

    //-DmaxProxies
    val maxProxies = System.getProperty("maxProxies")?.toIntOrNull()


    val workingProxies = mutableListOf<Proxy?>()

    // -DsqlHost
    val host = System.getProperty("sqlHost")
    // -DsqlPort
    val sqlPort = System.getProperty("sqlPort")?.toIntOrNull()
    // -DsqlUser
    val user = System.getProperty("sqlUser")
    // -DsqlPassword
    val password = System.getProperty("sqlPassword")

    Database.init(host = host, port = sqlPort, user = user, pass = password)

//    val profilesWithException = doSQL {
//        val profiles = mutableListOf<Int>()
//        val results = prepareStatement("SELECT ID FROM ${Database.TABLE_NAME} WHERE RESULT = 'EXCEPTION'").executeQuery()
//        while (results.next()) {
//            profiles.add(results.getInt(1))
//        }
//        profiles
//    }
//
//    if (profilesWithException.isNotEmpty()) {
//        println("${profilesWithException.size} profiles have run with exceptions, running them again!")
//        profilesWithException.forEachIndexed { index, profileID ->
//            val profile = getEnjinProfile(profileID)
//            println("(${index + 1}/${profilesWithException.size}) Got profile ${profile.id}, result is ${profile.result.name}")
//            Database.insertProfileOrUpdateIfExists(profile)
//        }
//    }

    val uncompletedProfiles = doSQL {
        val profiles = mutableListOf<Int>()
        val results = prepareStatement("SELECT ID FROM ${Database.TABLE_NAME} WHERE RESULT = 'PLACEHOLDER'").executeQuery()
        while (results.next()) {
            profiles.add(results.getInt(1))
        }
        profiles
    }
    if (uncompletedProfiles.isNotEmpty()) {
        println("Found ${uncompletedProfiles.size} incomplete profiles, completing them right now...")
        uncompletedProfiles.forEachIndexed { index, profileID ->
            getProfileAndUploadToDB(profileId = profileID, proxy = null, usePlaceholder = false, prefix = "(${index + 1}/${uncompletedProfiles.size})")
        }
    }

    val mostRecentProfile = System.getProperty("startProfile")?.toIntOrNull() ?: (Database.getLatestProfileID() + 1)

    if (proxies.isNotEmpty()) {
        run out@ {
            proxies.forEachIndexed { index, proxy ->
                var valid = false
                print("Testing proxy ${index + 1}/${proxies.size} $proxy... ")
                val split = proxy.split(":")
                if (split.size == 2) {
                    val url = split[0]
                    val port = split[1].toInt()

                    val httpProxy = Proxy(Proxy.Type.HTTP, InetSocketAddress(url, port))
                    val socksProxy = Proxy(Proxy.Type.SOCKS, InetSocketAddress(url, port))
                    if (testProxy(httpProxy)) {     // test http proxy first
                        workingProxies.add(httpProxy)
                        valid = true
                    } else if (testProxy(socksProxy)) {
                        workingProxies.add(socksProxy)
                        valid = true
                    }
                }
                println(valid)
                // max proxies hit
                if (maxProxies != null && workingProxies.size >= maxProxies) {
                    return@out
                }
            }
        }
        if (maxProxies != null) {
            println("Target proxy size is $maxProxies, obtained ${workingProxies.size} working proxies")
        } else {
            println("${workingProxies.size}/${proxies.size} proxies are working")
        }
        println()
    } else {
        println("Running without proxies\n")
    }

    lastUploadTime.set(System.currentTimeMillis())

    println("Latest profile is $mostRecentProfile, starting from ${mostRecentProfile + 1}")
    lastProfile.set(mostRecentProfile)

    if (System.getProperty("useDirect")?.toBoolean() == true) {
        workingProxies.add(0, null) // the first one uses user's direct connection
    }

    if (workingProxies.isNotEmpty() && Database.ds is SQLiteDataSource) {
        // for concurrent writers on SQLite
        doSQL { prepareStatement("pragma journal_mode=wal").execute() }
    }

    workingProxies.forEachIndexed { index, proxy ->
        thread {
            var exceptionCounter = 0    // if this reaches 10 (errors in a row), make the thread sleep for half an hour
            while (lastProfile.get() <= newestProfile) {
                val id = lastProfile.getAndIncrement()
                val isSuccessful = getProfileAndUploadToDB(profileId = id, proxy = proxy, usePlaceholder = true, prefix = "[THREAD $index]")
                if (isSuccessful) {
                    exceptionCounter = 0
                } else {
                    exceptionCounter++
                }
                if (exceptionCounter >= 10) {
                    println("Thread $index got 10 exceptions in a row, sleeping for 30 minutes")
                    exceptionCounter = 0
                    Thread.sleep(TimeUnit.MILLISECONDS.toMinutes(30))
                }
            }
        }
    }
}


fun addToWaitingProfilesOrUpload(profile: EnjinProfile, forceUpload: Boolean = false) {
    queue.add(profile)
    val time = System.currentTimeMillis()
    if (forceUpload || System.currentTimeMillis() >= lastUploadTime.get() + DELAY) {
        lastUploadTime.set(time)
        val toProcess = queue.toTypedArray()
        queue.clear()
        var statement: PreparedStatement? = null
        doSQL {
            autoCommit = false
            toProcess.forEach { profile ->
                statement = Database.insertProfileOrUpdateIfExists(profile, this, statement)
            }
            statement?.let {
                it.executeBatch()
                commit()
                val amount = toProcess.distinctBy { profile -> profile.id }.size
                println(buildString {
                    append("Saved $amount different profiles")
                    if (amount > 0) {
                        val min = toProcess.minOf { profile -> profile.id }
                        val max = toProcess.maxOf { profile -> profile.id }
                        append(" ($min -> $max)")
                    }
                })
            }
        }
    }
}

/**
 * Retrieves information for an Enjin profile and places it in a queue to be placed in the database
 *
 * @return Whether the result was successful (no exceptions generated)
 */
fun getProfileAndUploadToDB(profileId: Int, proxy: Proxy? = null, usePlaceholder: Boolean = true, prefix: String? = null): Boolean {
    if (usePlaceholder) {
        // mechanism to go back if scrape was unfinished?
        val placeholder = EnjinProfile().apply {
            id = profileId
            result = Result.PLACEHOLDER
        }
//        Database.insertProfileOrUpdateIfExists(placeholder)
        addToWaitingProfilesOrUpload(placeholder)
    }

    val profile = getEnjinProfile(profileId, proxy)
    printlnOnDebug(buildString {
        if (prefix != null) {
            append("$prefix \t")
        }
        append("Got profile ${profile.id}, result is ${profile.result.name}")
    })
//    Database.insertProfileOrUpdateIfExists(profile)
    addToWaitingProfilesOrUpload(profile)
    return profile.result != Result.EXCEPTION
}

fun testProxy(proxy: Proxy): Boolean {
    return try {
        Jsoup.connect("http://minecade.com/profile/1")
            .proxy(proxy)
            .get()
        true
    } catch (e: Exception) {
        false
    }
}

fun getEnjinProfile(profileId: Int, proxy: Proxy? = null): EnjinProfile {
    return enjinProfile {
        id = profileId

        val doc = try {
            Jsoup.connect("http://minecade.com/profile/$profileId/info")
                .apply {
                    if (proxy != null) {
                        this.proxy(proxy)
                    }
                }
                .get()
        } catch (e: Exception) {
            result = Result.EXCEPTION
            return@enjinProfile
        }

        if (doc.location().endsWith("minecade.com/")) {
            result = Result.DEACTIVATED
            return@enjinProfile
        }
        if (doc.select("h2.title").isNotEmpty()) {
            result = Result.PROFILE_ERROR
            return@enjinProfile
        }

        val cells = doc.select("div.cell")
        val profileViewCells = cells.first() ?: return@enjinProfile
        val cleaned = profileViewCells.text().cleanHtml()
        profileViews = NumberFormat.getNumberInstance(Locale.US).parse(cleaned).toInt()

        displayName = doc.select("span.cover_header_name_text").first()?.text()
        quote = doc.select("input.cover_header_quote_input").first()?.attr("value")?.take(255)
        if (quote?.isEmpty() == true) {
            quote = doc.select("span.cover_header_quote_text").first()?.text() ?: quote
        }
        val lastSeen = doc.select("div.cover_header_online").first()?.text()
        this.lastSeen = lastSeen?.let { getLastSeenTimestamp(it) } ?: this.lastSeen

        friends =
            doc.select("a.friends_popup_button").first()?.text()?.trim()
                ?.split(" ")?.getOrNull(0)?.toIntOrNull() ?: friends

        val joinDateStr = doc.select("div.info_data_value").getOrNull(6)?.text()?.trim()
        if (joinDateStr != null) {
            val monthDay = MonthDay.parse(joinDateStr, joinDateFormat)
            val year = Year.parse(joinDateStr, joinDateFormat)
            val date = monthDay.atYear(year.value)
            joinDate = date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()
        }

        bio = doc.select("div.info_data_about_text_full").first()?.text()?.take(65535)

        // also matches names with more than one _, however it's impossible for a custom url to just show two underscores
        // and if it does, well that's an interesting case
        val regex = "https://www.minecade.com/profile/([a-z](?:_?[a-z0-9]*)*)/info".toRegex()
        if (doc.location().contains(regex)) {
            val pattern = regex.toPattern()
            val matcher = pattern.matcher(doc.location())
            if (matcher.find()) {
                customUrl = matcher.group(1)
            }
        }

        result = Result.SUCCESS
    }
}

fun enjinProfile(block: EnjinProfile.() -> Unit): EnjinProfile {
    return EnjinProfile().apply(block)
}

fun getLastSeenTimestamp(timestamp: String): Long {
    val now = LocalDateTime.now()
    // Last seen 3 days ago
    return if (timestamp.startsWith("Last seen")) {
        val stripped = timestamp.removePrefix("Last seen").trim()
        if (stripped.equals("Just now", true)) {
            return System.currentTimeMillis()
        }
        // 3 days ago
        val split = stripped.split(" ")
        if (split[2] == "ago") {
            val amount = split[0].toLong()
            val unit = when {
                split[1].startsWith("min") -> ChronoUnit.MINUTES
                split[1].startsWith("hour") -> ChronoUnit.HOURS
                split[1].startsWith("day") -> ChronoUnit.DAYS
                split[1].startsWith("week") -> ChronoUnit.WEEKS
                split[1].startsWith("month") -> ChronoUnit.MONTHS
                else -> ChronoUnit.FOREVER
            }
            now.minus(amount, unit).toEpochSecond(ZoneOffset.UTC)
        } else if (split[1] == "at") {
            // Mon at 10:16
            val lastDay = DayOfWeek.values().find { it.name.startsWith(split[0], true) }
            val time = split[2].split(":").map { it.toInt() }
            now.with(TemporalAdjusters.previous(lastDay!!))
                .withHour(time[0])
                .withMinute(time[1])
                .toEpochSecond(ZoneOffset.UTC) * 1000
        } else {
            // Apr 20, 13
            val monthDay = MonthDay.parse(stripped, dateFormat)
            val year = Year.parse(stripped, dateFormat)
            val date = monthDay.atYear(year.value)
            date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()
        }
    } else if (timestamp == "ONLINE") {
        now.toEpochSecond(ZoneOffset.UTC)
    } else {
        -1
    }
}

class EnjinProfile {
    var id: Int = -1
    var profileViews: Int = -1
    var displayName: String? = null
    var quote: String? = null
    var customUrl: String? = null
    var friends: Int = -1
    var lastSeen: Long = -1
    var joinDate: Long = -1
    var bio: String? = null
    var result: Result = Result.OTHER

    override fun toString(): String {
        return "id=$id,result=${result.name},profileViews=$profileViews,displayName=$displayName,quote=$quote,customURL=${customUrl},friends=$friends,lastSeen=$lastSeen,joinDate=$joinDate,bio=$bio"
    }
}

enum class Result {
    SUCCESS,
    DEACTIVATED,
    PROFILE_ERROR,
    EXCEPTION,
    PLACEHOLDER,
    OTHER;
}
