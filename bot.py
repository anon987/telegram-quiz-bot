import logging
import pandas as pd
import os
import asyncio
import database as db
import uuid

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CommandHandler,
    ContextTypes,
    filters,
    PollAnswerHandler,
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
        session_id = str(uuid.uuid4())
        context.bot_data['current_session_id'] = session_id
        await update.message.reply_text(f"📄 Processing your quiz... (Session ID: {session_id})")

        df = pd.read_excel(file_path)

        for i, row in df.iterrows():
            question = str(row.get("Question", "")).strip()
            options = [str(row.get(col, "")).strip() for col in ["A", "B", "C", "D"]]
            correct = str(row.get("Correct", "A")).strip().upper()

            if question and all(options) and correct in ["A", "B", "C", "D"]:
                correct_option_id = ord(correct) - ord("A")
                message = await context.bot.send_poll(
                    chat_id=GROUP_CHAT_ID,
                    question=question,
                    options=options,
                    type="quiz",
                    correct_option_id=correct_option_id,
                    is_anonymous=False,
                )
                # Save the correct answer for this poll
                context.bot_data[message.poll.id] = (correct_option_id, session_id)
                await asyncio.sleep(1)  # Prevent hitting Telegram rate limits
            else:
                logger.warning(f"⚠️ Skipping invalid row: {row}")

    except Exception as e:
        logger.error(f"❌ Error processing file: {e}")
        await update.message.reply_text(f"❌ Error: {e}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles a user's answer to a poll."""
    answer = update.poll_answer
    user = answer.user
    poll_id = answer.poll_id
    
    if poll_id in context.bot_data:
        correct_option_id, session_id = context.bot_data[poll_id]
        is_correct = answer.option_ids[0] == correct_option_id
        db.log_answer(user.id, user.username, is_correct, session_id)

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Please specify a time frame (daily, weekly, monthly, yearly) or 'session' for the last quiz session.")
        return

    arg = context.args[0].lower()
    
    if arg == 'session':
        session_id = context.bot_data.get('current_session_id')
        if not session_id:
            await update.message.reply_text("No active quiz session found.")
            return
        leaderboard_data = db.get_leaderboard(session_id=session_id)
        message = f"🏆 *Current Session Leaderboard* 🏆\n\n"
    elif arg in ['daily', 'weekly', 'monthly', 'yearly', 'all']:
        leaderboard_data = db.get_leaderboard(time_frame=arg)
        message = f"🏆 *{arg.capitalize()} Leaderboard* 🏆\n\n"
    else:
        await update.message.reply_text("Invalid argument. Please use 'daily', 'weekly', 'monthly', 'yearly', or 'session'.")
        return

    if not leaderboard_data:
        await update.message.reply_text(f"No data available for the selected leaderboard.")
        return

    for i, row in enumerate(leaderboard_data):
        message += f"{i+1}. {row['username']} - {row['correct']} correct, {row['wrong']} wrong\n"

    await update.message.reply_text(message, parse_mode='Markdown')

# Main entry point
def main():
    db.initialize_database()
    if not BOT_TOKEN or not GROUP_CHAT_ID:
        raise ValueError("BOT_TOKEN or GROUP_CHAT_ID is not set in environment variables.")

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(PollAnswerHandler(handle_poll_answer))

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
