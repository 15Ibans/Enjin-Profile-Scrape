package me.iban.enjinprofilescrape

import org.jsoup.Jsoup
import org.jsoup.safety.Safelist

fun String.cleanHtml(): String {
    return Jsoup.clean(this, Safelist.basic())
}