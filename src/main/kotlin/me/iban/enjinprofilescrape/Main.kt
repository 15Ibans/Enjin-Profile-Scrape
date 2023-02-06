package me.iban.enjinprofilescrape

import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import java.net.SocketTimeoutException
import java.text.NumberFormat
import java.time.*
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAdjusters
import java.util.*

val dateFormat = DateTimeFormatter.ofPattern("MMM d, yy")
    .withLocale(Locale.getDefault())
    .withZone(ZoneId.systemDefault())

val joinDateFormat = DateTimeFormatter.ofPattern("d MMMM yyyy")
    .withLocale(Locale.getDefault())
    .withZone(ZoneId.systemDefault())

val newestProfile = 21024144

fun main(args: Array<String>) {
    Database.init()
    val mostRecentProfile = Database.getLatestProfileID()
    println("Latest profile is $mostRecentProfile, starting from ${mostRecentProfile + 1}")

    for (i in mostRecentProfile + 1..newestProfile) {
        val profile = getEnjinProfile(i)
        println("Got new profile ${profile.id}, result is ${profile.result.name}")
        println(profile.toString())
//        Database.insertProfileOrUpdateIfExists(profile)
    }
}

fun getEnjinProfile(profileId: Int): EnjinProfile {
    return enjinProfile {
        id = profileId

        val doc = try {
            Jsoup.connect("http://minecade.com/profile/$profileId/info").get()
        } catch (e: SocketTimeoutException) {
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
        val cleaned = Jsoup.clean(profileViewCells.html(), Safelist.basic())
        profileViews = NumberFormat.getNumberInstance(Locale.US).parse(cleaned).toInt()

        displayName = doc.select("span.cover_header_name_text").first()?.html()
        quote = doc.select("span.cover_header_quote_text").first()?.html()
        val lastSeen = doc.select("div.cover_header_online").first()?.html()
        this.lastSeen = lastSeen?.let { getLastSeenTimestamp(it) } ?: this.lastSeen

        friends =
            doc.select("a.friends_popup_button").first()?.html()?.trim()?.removeSuffix("Friends")?.trim()?.toIntOrNull()
                ?: friends

        val joinDateStr = doc.select("div.info_data_value").getOrNull(6)?.html()?.trim()
        if (joinDateStr != null) {
            val monthDay = MonthDay.parse(joinDateStr, joinDateFormat)
            val year = Year.parse(joinDateStr, joinDateFormat)
            val date = monthDay.atYear(year.value)
            joinDate = date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()
        }

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
    var result: Result = Result.OTHER

    override fun toString(): String {
        return "id=$id,result=${result.name},profileViews=$profileViews,displayName=$displayName,quote=$quote,customURL=${customUrl},friends=$friends,lastSeen=$lastSeen,joinDate=$joinDate"
    }
}

enum class Result {
    SUCCESS,
    DEACTIVATED,
    PROFILE_ERROR,
    OTHER;
}