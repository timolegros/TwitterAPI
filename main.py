import tweepy
import datetime as dt

TWaccessToken = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
TWaccessTokenSecret = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
TWapiKey = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
TWapiSecretKey = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'


def getAPI():
    auth = tweepy.OAuthHandler(TWapiKey, TWapiSecretKey)
    auth.set_access_token(TWaccessToken, TWaccessTokenSecret)

    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    return api


def getTwitterData(api, searchTerm, snowFlakeCurrent, snowFlakeEnd):
    """
    Pulls the twitter data given api, search term, and start/end points
    :param api: tweepy API generated in getAPI()
    :param searchTerm: the term/phrase to search in string format
    :param snowFlakeCurrent: snowflake of current/near current date
    :param snowFlakeEnd: snowflake of the last day to retrieve tweets from
    :return: list containing all the data with datetime in
    """

    data = []

    for tweet in tweepy.Cursor(api.search, q=searchTerm, result_type='recent', max_id=snowFlakeCurrent,
                               since_id=snowFlakeEnd, count=100).items(18000):
        # createdTime = tweet.created_at.strftime('%Y-%m-%d %H:%M')
        # createdTime = dt.datetime.strptime(createdTime, '%Y-%m-%d %H:%M').replace(tzinfo=pytz.UTC)
        # data.append(createdTime)
        data.append(tweet)

    return data


def timeToSnowFlake(endDate):
    """
    Takes the UTC end date in '2020-07-02 21:58:00+00:00' format and converts it to a local time epoch then snowflake.
    Can also take UTC end date in '2020-07-02 21:58:00' format to convert to epoch and then snowflake
    :param endDate: UTC date in '2020-07-02 21:58:00+00:00' or '2020-07-02 21:58:00' format
    :return: snowflake converted from local timezone epoch -- this is so snowflake matches local time
    """
    timeStamp = endDate.timestamp()
    snowFlake = (int(round(timeStamp * 1000)) - 1288834974657) << 22
    return snowFlake


def runTwitterScraper(searchTerm, currentTime, endDate):
    api = getAPI()
    currentSnowFlake = timeToSnowFlake(currentTime)
    endSnowFlake = timeToSnowFlake(endDate)

    data, someAPI = getTwitterData(api, searchTerm, currentSnowFlake, endSnowFlake)

    return data, someAPI


def snowflake2utc(sf):
    return ((sf >> 22) + 1288834974657) / 1000.0


if __name__ == '__main__':
    startDate = dt.datetime(2020, 9, 5, 12, 0, 0, 0)
    endDate = dt.datetime(2020, 9, 5, 1, 0, 0, 0)

    data, api = runTwitterScraper('$AAPL', startDate, endDate)

