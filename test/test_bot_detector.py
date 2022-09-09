from analog.bot_detector import BotDetector

LINKEDIN = 'LinkedInBot'
SAFARI = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 '
    '(KHTML, like Gecko) Version/15.6.1 Safari/605.1.15'
)


def test_bot_detector(inspect: bool = False) -> None:
    is_bot = BotDetector()
    if inspect:
        is_bot.inspect()

    assert not is_bot.test(SAFARI)
    assert is_bot.lookup(SAFARI) is None
    assert is_bot.test(LINKEDIN)
    bot = is_bot.lookup(LINKEDIN)
    assert isinstance(bot, dict)
    assert bot['name'] == 'LinkedIn Bot'
    assert bot['category'] == 'Social Media Agent'
    assert bot['url'] == 'http://www.linkedin.com'


if __name__ == '__main__':
    test_bot_detector(True)
