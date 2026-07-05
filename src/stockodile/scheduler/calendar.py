"""Trading calendar and hours utility for US Equity markets."""

import datetime
from zoneinfo import ZoneInfo

# US Equity Market Timezone is Eastern Time
MARKET_TZ = ZoneInfo("America/New_York")


def easter_date(year: int) -> datetime.date:
    """Calculate the date of Easter Sunday using Meeus/Jones/Butcher algorithm."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    L = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * L) // 451
    month = (h + L - 7 * m + 114) // 31
    day = ((h + L - 7 * m + 114) % 31) + 1
    return datetime.date(year, month, day)


def nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> datetime.date | None:
    """Find the n-th occurrence of a weekday in a given month.

    weekday: 0 = Monday, ..., 6 = Sunday.
    n: 1-indexed (e.g. 1 for first occurrence, 3 for third, etc.).
    """
    if n <= 0:
        return None
    try:
        first_day = datetime.date(year, month, 1)
        first_weekday = first_day.weekday()
        days_to_first = (weekday - first_weekday) % 7
        target_day = 1 + days_to_first + (n - 1) * 7
        res = datetime.date(year, month, target_day)
        if res.month != month:
            return None
        return res
    except ValueError:
        return None


def last_weekday_of_month(year: int, month: int, weekday: int) -> datetime.date:
    """Find the last occurrence of a weekday in a given month.

    weekday: 0 = Monday, ..., 6 = Sunday.
    """
    if month == 12:
        last_day = datetime.date(year, 12, 31)
    else:
        last_day = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
    last_weekday = last_day.weekday()
    days_to_subtract = (last_weekday - weekday) % 7
    return last_day - datetime.timedelta(days=days_to_subtract)


def get_observed_holiday(year: int, month: int, day: int) -> datetime.date:
    """Return the observed date of a holiday if it falls on a weekend.

    If Saturday, observed on Friday before.
    If Sunday, observed on Monday after.
    """
    dt = datetime.date(year, month, day)
    wd = dt.weekday()
    if wd == 5:  # Saturday
        return dt - datetime.timedelta(days=1)
    if wd == 6:  # Sunday
        return dt + datetime.timedelta(days=1)
    return dt


class USMarketCalendar:
    """Utility class to determine trading days and market hours for US equities."""

    def __init__(self, custom_holidays: set[datetime.date] | None = None) -> None:
        self.custom_holidays = custom_holidays or set()

    def get_holidays(self, year: int) -> set[datetime.date]:
        """Get the set of observed US stock market holidays for a given year."""
        holidays = set()

        # 1. New Year's Day (observed)
        # Note: If Jan 1 is Saturday, Dec 31 of prior year is observed.
        ny_observed = get_observed_holiday(year, 1, 1)
        holidays.add(ny_observed)
        if datetime.date(year, 1, 1).weekday() == 5:
            # December 31 of prior year is observed for this year's New Year
            holidays.add(datetime.date(year - 1, 12, 31))
        # Also check if next year's New Year's Day is observed on Dec 31 of this year
        next_ny = datetime.date(year + 1, 1, 1)
        if next_ny.weekday() == 5:
            holidays.add(datetime.date(year, 12, 31))

        # 2. Martin Luther King Jr. Day (3rd Monday in Jan)
        if year >= 1998:
            mlk = nth_weekday_of_month(year, 1, 0, 3)
            if mlk is not None:
                holidays.add(mlk)

        # 3. Washington's Birthday / Presidents' Day (3rd Monday in Feb)
        presidents = nth_weekday_of_month(year, 2, 0, 3)
        if presidents is not None:
            holidays.add(presidents)

        # 4. Good Friday (Friday before Easter)
        easter = easter_date(year)
        holidays.add(easter - datetime.timedelta(days=2))

        # 5. Memorial Day (last Monday in May)
        holidays.add(last_weekday_of_month(year, 5, 0))

        # 6. Juneteenth National Independence Day (June 19, observed)
        if year >= 2022:
            holidays.add(get_observed_holiday(year, 6, 19))

        # 7. Independence Day (July 4, observed)
        holidays.add(get_observed_holiday(year, 7, 4))

        # 8. Labor Day (1st Monday in Sept)
        labor = nth_weekday_of_month(year, 9, 0, 1)
        if labor is not None:
            holidays.add(labor)

        # 9. Thanksgiving Day (4th Thursday in Nov)
        thanksgiving = nth_weekday_of_month(year, 11, 3, 4)
        if thanksgiving is not None:
            holidays.add(thanksgiving)

        # 10. Christmas Day (Dec 25, observed)
        holidays.add(get_observed_holiday(year, 12, 25))

        return holidays | self.custom_holidays

    def is_holiday(self, date_val: datetime.date) -> bool:
        """Check if a given date is a stock market holiday."""
        return date_val in self.get_holidays(date_val.year)

    def is_early_close(self, date_val: datetime.date) -> bool:
        """Check if the market closes early (1:00 PM Eastern) on the given date."""
        # 1. Day after Thanksgiving (Black Friday)
        thanksgiving = nth_weekday_of_month(date_val.year, 11, 3, 4)
        if thanksgiving is not None:
            black_friday = thanksgiving + datetime.timedelta(days=1)
            if date_val == black_friday:
                return True

        # 2. Christmas Eve (Dec 24) if it's a weekday and not an observed holiday
        if date_val.month == 12 and date_val.day == 24:
            christmas = datetime.date(date_val.year, 12, 25)
            # If Christmas is Saturday, Dec 24 is the observed holiday (market closed)
            if christmas.weekday() != 5:
                return date_val.weekday() < 5

        # 3. Day before Independence Day (July 3) if July 4 is a weekday
        if date_val.month == 7 and date_val.day == 3:
            july4 = datetime.date(date_val.year, 7, 4)
            return july4.weekday() < 5

        return False

    def is_trading_day(self, date_val: datetime.date) -> bool:
        """Check if a date is a standard US stock market trading day (not weekend or holiday)."""
        if date_val.weekday() >= 5:  # Saturday or Sunday
            return False
        return not self.is_holiday(date_val)

    def get_market_hours(
        self, date_val: datetime.date
    ) -> tuple[datetime.datetime, datetime.datetime] | None:
        """Get the localized open and close datetimes for a given date.

        Returns None if the market is closed on that day.
        """
        if not self.is_trading_day(date_val):
            return None

        # Standard open is 9:30 AM ET
        open_time = datetime.time(9, 30)

        # Close time is 1:00 PM on early close, otherwise 4:00 PM
        if self.is_early_close(date_val):
            close_time = datetime.time(13, 0)
        else:
            close_time = datetime.time(16, 0)

        open_dt = datetime.datetime.combine(date_val, open_time, tzinfo=MARKET_TZ)
        close_dt = datetime.datetime.combine(date_val, close_time, tzinfo=MARKET_TZ)

        return open_dt, close_dt

    def is_market_open(self, dt: datetime.datetime) -> bool:
        """Check if the market is open at the specified datetime."""
        # Convert to Eastern Time timezone
        dt_et = dt.astimezone(MARKET_TZ)
        hours = self.get_market_hours(dt_et.date())
        if hours is None:
            return False

        open_dt, close_dt = hours
        return open_dt <= dt_et <= close_dt
