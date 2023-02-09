package me.iban.enjinprofilescrape

import org.jsoup.Jsoup
import org.sqlite.SQLiteDataSource
import java.text.NumberFormat
import java.time.*
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAdjusters
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

val dateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("MMM d, yy")
    .withLocale(Locale.getDefault())
    .withZone(ZoneId.systemDefault())

val joinDateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("d MMMM yyyy")
    .withLocale(Locale.getDefault())
    .withZone(ZoneId.systemDefault())

const val newestProfile = 21024144

val lastProfile = AtomicInteger(-1)

fun main(args: Array<String>) {
    // -DstartProfile
    //proxies are in format 127.0.0.1:XXXXX,127.0.0.1:XXXXX,...
    // -Dproxies
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
//        val results = prepareStatement("SELECT ID FROM profiles WHERE RESULT = 'EXCEPTION'").executeQuery()
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
        val results = prepareStatement("SELECT ID FROM profiles WHERE RESULT = 'PLACEHOLDER'").executeQuery()
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
                print("(${index + 1}/${proxies.size}) Testing proxy $proxy... ")
                val split = proxy.split(":")
                if (split.size == 2) {
                    val url = split[0]
                    val port = split[1].toInt()

                    if (testProxy(url, port)) {
                        workingProxies.add(Proxy(url, port))
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

    println("Latest profile is $mostRecentProfile, starting from ${mostRecentProfile + 1}")
    lastProfile.set(mostRecentProfile)

    workingProxies.add(0, null) // the first one uses user's direct connection

    if (workingProxies.isNotEmpty() && Database.ds is SQLiteDataSource) {
        // for concurrent writers on SQLite
        doSQL { prepareStatement("pragma journal_mode=wal").execute() }
    }

    workingProxies.forEachIndexed { index, proxy ->
        thread {
            while (lastProfile.get() <= newestProfile) {
                val id = lastProfile.getAndIncrement()
                getProfileAndUploadToDB(profileId = id, proxy = proxy, usePlaceholder = true, prefix = "[THREAD $index]")
            }
        }
    }
}

fun getProfileAndUploadToDB(profileId: Int, proxy: Proxy? = null, usePlaceholder: Boolean = true, prefix: String? = null) {
    if (usePlaceholder) {
        // mechanism to go back if scrape was unfinished?
        val placeholder = EnjinProfile().apply {
            id = profileId
            result = Result.PLACEHOLDER
        }
        Database.insertProfileOrUpdateIfExists(placeholder)
    }

    val profile = getEnjinProfile(profileId, proxy)
    println(buildString {
        if (prefix != null) {
            append("$prefix \t")
        }
        append("Got profile ${profile.id}, result is ${profile.result.name}")
    })
    Database.insertProfileOrUpdateIfExists(profile)
}

fun testProxy(url: String, port: Int): Boolean {
    return try {
        Jsoup.connect("http://minecade.com/profile/1")
            .proxy(url, port)
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
                        proxy(proxy.url, proxy.port)
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
        val cleaned = profileViewCells.html().cleanHtml()
        profileViews = NumberFormat.getNumberInstance(Locale.US).parse(cleaned).toInt()

        displayName = doc.select("span.cover_header_name_text").first()?.html()
        quote = doc.select("span.cover_header_quote_text").first()?.html()
        val lastSeen = doc.select("div.cover_header_online").first()?.html()
        this.lastSeen = lastSeen?.let { getLastSeenTimestamp(it) } ?: this.lastSeen

        friends =
            doc.select("a.friends_popup_button").first()?.html()?.trim()
                ?.split(" ")?.getOrNull(0)?.toIntOrNull() ?: friends

        val joinDateStr = doc.select("div.info_data_value").getOrNull(6)?.html()?.trim()
        if (joinDateStr != null) {
            val monthDay = MonthDay.parse(joinDateStr, joinDateFormat)
            val year = Year.parse(joinDateStr, joinDateFormat)
            val date = monthDay.atYear(year.value)
            joinDate = date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()
        }

        bio = doc.select("div.info_data_about_text_full").first()?.html()?.take(65535)

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
        // 3 days ago
        val split = stripped.split(" ")
        if (split[2] == "ago") {
            val amount = split[0].toLong()
            val unit = when {
                split[1].startsWith("minute") -> ChronoUnit.MINUTES
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
                .toEpochSecond(ZoneOffset.UTC)
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

data class Proxy(val url: String, val port: Int)