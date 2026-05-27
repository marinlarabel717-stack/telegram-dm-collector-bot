from app.bot import DmCollectorBot, configure_logging
from app.config import get_settings


if __name__ == "__main__":
    configure_logging()
    settings = get_settings()
    bot = DmCollectorBot(settings)
    bot.run()
