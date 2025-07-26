import logging
import pandas as pd
from telegram.ext import Updater, MessageHandler, Filters, CommandHandler
import os

BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_CHAT_ID = os.getenv("GROUP_CHAT_ID")  # Use -100... or @channelusername

logging.basicConfig(level=logging.INFO)

def start(update, context):
    update.message.reply_text("Send me your quiz Excel file (.xlsx)")

def handle_document(update, context):
    file = update.message.document
    if not file.file_name.endswith('.xlsx'):
        update.message.reply_text("Please upload a valid .xlsx Excel file.")
        return

    file_path = f"./{file.file_name}"
    file.get_file().download(file_path)
    update.message.reply_text("Processing your quiz...")

    try:
        df = pd.read_excel(file_path)
        for i, row in df.iterrows():
            question = row['Question']
            options = [row['A'], row['B'], row['C'], row['D']]
            correct_option_id = ord(str(row['Correct']).upper()) - ord('A')

            context.bot.send_poll(
                chat_id=GROUP_CHAT_ID,
                question=question,
                options=options,
                type='quiz',
                correct_option_id=correct_option_id,
                is_anonymous=False
            )
    except Exception as e:
        update.message.reply_text(f"Error processing file: {e}")
    finally:
        os.remove(file_path)

def main():
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler('start', start))
    dp.add_handler(MessageHandler(Filters.document, handle_document))

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
