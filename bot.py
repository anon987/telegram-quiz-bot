import logging
import pandas as pd
import os
import asyncio

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CommandHandler,
    ContextTypes,
    filters,
)

# Load from environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_CHAT_ID = os.getenv("GROUP_CHAT_ID")  # e.g., -1001234567890 or @channelusername

# Set up logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# /start command
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Hi! Please send me your quiz Excel file (.xlsx)")

# Handle uploaded Excel file
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file = update.message.document
    if not file or not file.file_name.endswith(".xlsx"):
        await update.message.reply_text("⚠️ Please upload a valid `.xlsx` Excel file.")
        return

    file_path = f"./{file.file_name}"
    new_file = await file.get_file()

    try:
        await new_file.download_to_drive(file_path)
        await update.message.reply_text("📄 Processing your quiz...")

        df = pd.read_excel(file_path)

        for i, row in df.iterrows():
            question = str(row.get("Question", "")).strip()
            options = [str(row.get(col, "")).strip() for col in ["A", "B", "C", "D"]]
            correct = str(row.get("Correct", "A")).strip().upper()

            if question and all(options) and correct in ["A", "B", "C", "D"]:
                correct_option_id = ord(correct) - ord("A")
                await context.bot.send_poll(
                    chat_id=GROUP_CHAT_ID,
                    question=question,
                    options=options,
                    type="quiz",
                    correct_option_id=correct_option_id,
                    is_anonymous=False,
                )
                await asyncio.sleep(1)  # Prevent hitting Telegram rate limits
            else:
                logger.warning(f"⚠️ Skipping invalid row: {row}")

    except Exception as e:
        logger.error(f"❌ Error processing file: {e}")
        await update.message.reply_text(f"❌ Error: {e}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

# Main entry point
def main():
    if not BOT_TOKEN or not GROUP_CHAT_ID:
        raise ValueError("BOT_TOKEN or GROUP_CHAT_ID is not set in environment variables.")

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # Set up webhook
    webhook_url = os.getenv("WEBHOOK_URL")
    port = int(os.getenv("PORT", "8443"))
    if webhook_url:
        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=BOT_TOKEN,
            webhook_url=f"{webhook_url}/{BOT_TOKEN}"
        )
    else:
        logger.warning("WEBHOOK_URL not set, running in polling mode.")
        app.run_polling()

if __name__ == "__main__":
    main()
