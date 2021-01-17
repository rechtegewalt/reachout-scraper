import re

import dataset
import get_retries
from bs4 import BeautifulSoup
from dateparser import parse

db = dataset.connect("sqlite:///data.sqlite")

tab_incidents = db["incidents"]
tab_sources = db["sources"]
tab_chronicles = db["chronicles"]


tab_chronicles.upsert(
    {
        "iso3166_1": "DE",
        "iso3166_2": "DE-BE",
        "chronicler_name": "ReachOut",
        "chronicler_description": "ReachOut ist eine Beratungsstelle für Opfer rechter, rassistischer und antisemitischer Gewalt in Berlin.",
        "chronicler_url": "https://www.reachoutberlin.de",
        "chronicle_source": "https://www.reachoutberlin.de/de/chronik",
    },
    ["chronicler_name"],
)


BASE_URL = "https://www.reachoutberlin.de/de/chronik"


def fix_date_typo(x):
    """
    fix date typos such as 14,12.2020 or 01.12,2020
    """
    x = re.sub(r"(\d\d),(\d\d.\d\d\d\d)", r"\1.\2", x)
    x = re.sub(r"(\d\d.\d\d),(\d\d\d\d)", r"\1.\2", x)
    return x


def fetch(url):
    html_content = get_retries.get(url, verbose=True, max_backoff=128).text
    soup = BeautifulSoup(html_content, "lxml")
    return soup


def process_report(tr):
    date = parse(
        tr.select_one(".views-field-field-vorfallsdatum-1").get_text(), languages=["de"]
    )

    location_link = tr.select_one(".views-field-title a")
    url = rg_id = "https://www.reachoutberlin.de" + location_link.get("href")
    city = location_link.get_text().replace("Internet", "").strip()

    ps = tr.select(".views-field.views-field-body p")

    if len(ps) == 0:
        raise ValueError("xxx", tr)

    sources = []
    if len(ps) > 1:
        description = ps[0].get_text().strip()
        raw_sources = []
        for p in ps[1:]:
            raw_sources += list(p.strings)

    else:
        the_strings = list(ps[0].strings)
        description, raw_sources = the_strings[0].strip(), the_strings[1:]

    for x in raw_sources:
        x = fix_date_typo(x)
        if "," in x:
            s_name, s_dates = x.split(",")[0], x.split(",")[1:]

            some_valid_date = False
            for d in s_dates:
                s_date = parse(d, languages=["de"])
                if s_date is not None:
                    some_valid_date = True
                    sources.append(dict(rg_id=rg_id, name=s_name.strip(), date=s_date))

            if not some_valid_date:
                sources.append(dict(rg_id=rg_id, name=s_name.strip()))

        else:
            sources.append(dict(rg_id=rg_id, name=x.strip()))

    # county is always Berlin
    data = dict(
        url=url,
        rg_id=rg_id,
        date=date,
        city=city,
        county="Berlin",
        description=description,
        chronicler_name="ReachOut",
    )

    print(data)

    tab_incidents.upsert(data, ["rg_id"])

    for x in sources:
        tab_sources.upsert(x, ["rg_id", "name", "date"])


def process_page(page):
    for tr in page.select("div.content tbody tr"):
        process_report(tr)


initial_soup = fetch(BASE_URL)

last_page = re.findall(
    r"\d+", initial_soup.select_one("li.pager-last.last a").get("href")
)[0]

last_page = int(last_page)

process_page(initial_soup)

print(last_page)

i = 1
while i <= last_page:
    url = BASE_URL + f"?page={i}"
    print(url)
    process_page(fetch(url))
    i += 1
