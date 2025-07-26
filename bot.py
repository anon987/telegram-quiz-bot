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

BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_CHAT_ID = os.getenv("GROUP_CHAT_ID")  # e.g., -1001234567890 or @channelusername

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

# /start command handler
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Send me your quiz Excel file (.xlsx)")

# Document upload handler
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file = update.message.document
    if not file.file_name.endswith(".xlsx"):
        await update.message.reply_text("Please upload a valid .xlsx Excel file.")
        return

    file_path = f"./{file.file_name}"
    new_file = await file.get_file()
    await new_file.download_to_drive(file_path)
    await update.message.reply_text("Processing your quiz...")

    try:
        df = pd.read_excel(file_path)
        for i, row in df.iterrows():
            question = row["Question"]
            options = [row["A"], row["B"], row["C"], row["D"]]
            correct_option_id = ord(str(row["Correct"]).upper()) - ord("A")

            await context.bot.send_poll(
                chat_id=GROUP_CHAT_ID,
                question=question,
                options=options,
                type="quiz",
                correct_option_id=correct_option_id,
                is_anonymous=False,
            )
            await asyncio.sleep(1)  # Delay between polls to avoid rate limits
    except Exception as e:
        await update.message.reply_text(f"Error processing file: {e}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

# Main function to run the bot
def main():
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    application.run_polling()

if __name__ == "__main__":
    main()
