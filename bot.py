import logging
import pandas as pd
import os
import asyncio
import database as db
import uuid
import re
from telegram.helpers import escape_markdown
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
GROUP_CHAT_ID = os.getenv("GROUP_CHAT_ID")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")

# Set up logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def escape_markdown_v1(text: str) -> str:
    """Escapes characters for Telegram's legacy Markdown."""
    if not text:
        return ""
    # Characters to escape for legacy Markdown
    escape_chars = ['_', '*', '`', '[']
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    return text


def validate_text_length(text: str, max_length: int, field_name: str) -> bool:
    """Validates text length and logs warnings if exceeded."""
    if len(text) > max_length:
        logger.warning(f"⚠️ {field_name} exceeds {max_length} characters: '{text[:50]}...'")
        return False
    return True


def get_answer_option_id(answer_english: str, answer_hindi: str) -> int:
    """
    Determines the correct option ID from English or Hindi answer text.
    Returns the option index (0-3) for options A-D.
    """
    # Check English answer first
    if answer_english and answer_english.strip().upper() in ['A', 'B', 'C', 'D']:
        return ord(answer_english.strip().upper()) - ord('A')
    
    # Check Hindi answer patterns
    if answer_hindi:
        hindi_answer = answer_hindi.strip()
        # Common Hindi patterns for options
        hindi_patterns = {
            'अ': 0, 'A': 0, 'ए': 0,
            'ब': 1, 'B': 1, 'बी': 1,
            'स': 2, 'C': 2, 'सी': 2,
            'द': 3, 'D': 3, 'डी': 3
        }
        
        for pattern, option_id in hindi_patterns.items():
            if pattern in hindi_answer:
                return option_id
    
    # Default to option A if unable to parse
    logger.warning(f"Unable to parse answer: English='{answer_english}', Hindi='{answer_hindi}'. Defaulting to A.")
    return 0


# /start command
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Hi! Please send me your quiz Excel file (.xlsx)")

# Handle uploaded Excel file
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if str(user_id) != ADMIN_USER_ID:
        await update.message.reply_text("Sorry, only the admin can start a new quiz.")
        return

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
        
        processed_count = 0
        skipped_count = 0

        for i, row in df.iterrows():
            try:
                # Extract data from the new format
                question_no = str(row.get("No.", "")).strip()
                question_english = str(row.get("Question (English)", "")).strip()
                question_hindi = str(row.get("प्रश्न (Hindi)", "")).strip()
                
                # Combine English and Hindi questions
                question = f"{question_english}"
                if question_hindi and question_hindi != "nan":
                    question += f"\n\n{question_hindi}"
                
                # Extract options (English and Hindi)
                options = []
                option_labels = ['A', 'B', 'C', 'D']
                
                for label in option_labels:
                    option_eng = str(row.get(f"Option {label}", "")).strip()
                    option_hin = str(row.get(f"विकल्प {label}", "")).strip()
                    
                    combined_option = option_eng
                    if option_hin and option_hin != "nan":
                        combined_option += f" / {option_hin}"
                    
                    options.append(combined_option)
                
                # Extract explanations
                explanation_english = str(row.get("Explanation (English)", "")).strip()
                explanation_hindi = str(row.get("व्याख्या (Hindi)", "")).strip()
                
                explanation = ""
                if explanation_english and explanation_english != "nan":
                    explanation = explanation_english
                if explanation_hindi and explanation_hindi != "nan":
                    if explanation:
                        explanation += f"\n\n{explanation_hindi}"
                    else:
                        explanation = explanation_hindi
                
                # Extract correct answers
                answer_english = str(row.get("Answer (English)", "")).strip()
                answer_hindi = str(row.get("उत्तर (Hindi)", "")).strip()
                
                # Validate lengths
                if not validate_text_length(question, 300, f"Question #{question_no}"):
                    skipped_count += 1
                    continue
                
                # Check if any option exceeds 100 characters
                option_length_valid = True
                for j, option in enumerate(options):
                    if not validate_text_length(option, 100, f"Option {option_labels[j]} for Question #{question_no}"):
                        option_length_valid = False
                        break
                
                if not option_length_valid:
                    skipped_count += 1
                    continue
                
                # Validate that we have all required data
                if not question or not all(opt.strip() for opt in options):
                    logger.warning(f"⚠️ Skipping question #{question_no}: Missing question or options")
                    skipped_count += 1
                    continue
                
                # Get correct option ID
                correct_option_id = get_answer_option_id(answer_english, answer_hindi)
                
                # Send poll with explanation
                message = await context.bot.send_poll(
                    chat_id=GROUP_CHAT_ID,
                    question=question,
                    options=options,
                    type="quiz",
                    correct_option_id=correct_option_id,
                    is_anonymous=False,
                    explanation=explanation if explanation else None  # This adds the bulb icon with explanation
                )
                
                # Save the correct answer for this poll
                context.bot_data[message.poll.id] = (correct_option_id, session_id)
                processed_count += 1
                
                await asyncio.sleep(1)  # Prevent hitting Telegram rate limits
                
            except Exception as e:
                logger.error(f"❌ Error processing row {i+1}: {e}")
                skipped_count += 1
                continue

        # Send summary message
        summary = f"✅ Quiz processing completed!\n"
        summary += f"📊 Processed: {processed_count} questions\n"
        if skipped_count > 0:
            summary += f"⚠️ Skipped: {skipped_count} questions (length validation failed or missing data)"
        
        await update.message.reply_text(summary)

    except Exception as e:
        logger.error(f"❌ Error processing file: {e}")
        await update.message.reply_text(f"❌ Error: {e}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


# Handle poll answers
async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles a user's answer to a poll."""
    answer = update.poll_answer
    user = answer.user
    poll_id = answer.poll_id

    if poll_id in context.bot_data:
        correct_option_id, session_id = context.bot_data[poll_id]
        is_correct = answer.option_ids[0] == correct_option_id
        db.log_answer(user.id, user.username, is_correct, session_id)


# /leaderboard command
async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Please specify a time frame (daily, weekly, monthly, all) or 'session' for the last quiz.")
        return

    arg = context.args[0].lower()

    if arg == 'session':
        session_id = context.bot_data.get('current_session_id')
        if not session_id:
            await update.message.reply_text("No active quiz session found.")
            return
        leaderboard_data = db.get_leaderboard(session_id=session_id)
        message = f"🏆 *Current Session Leaderboard* 🏆\n\n"
    elif arg in ['daily', 'weekly', 'monthly', 'all']:
        leaderboard_data = db.get_leaderboard(time_frame=arg)
        message = f"🏆 *{arg.capitalize()} Leaderboard* 🏆\n\n"
    else:
        await update.message.reply_text("Invalid argument. Please use 'daily', 'weekly', 'monthly', 'all', or 'session'.")
        return

    if not leaderboard_data:
        await update.message.reply_text(f"No data available for the selected leaderboard.")
        return

    for i, row in enumerate(leaderboard_data):
        username = escape_markdown_v1(str(row.get('username', 'Unknown')))
        message += f"{i+1}. {username} - {row['correct']} correct, {row['wrong']} wrong\n"

    await update.message.reply_text(message, parse_mode='Markdown')


def escape_markdown_v2(text: str) -> str:
    """
    Escapes all special characters for Telegram MarkdownV2.
    """
    if not text:
        return ""
    # Telegram MarkdownV2 special characters - order matters!
    # Escape backslash first, then other characters
    text = text.replace('\\', '\\\\')
    # Then escape all other special characters
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

# Also update your groupinfo function to handle member_count properly:
async def groupinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat:
        await update.message.reply_text("⚠️ Could not fetch chat details.")
        return

    # Escape all dynamic text
    title = escape_markdown_v2(chat.title or "N/A")
    username = f"@{escape_markdown_v2(chat.username)}" if chat.username else "N/A"

    try:
        member_count = await context.bot.get_chat_member_count(chat.id)
        member_count_str = str(member_count)  # Numbers don't need escaping
    except Exception as e:
        member_count_str = f"Error: {escape_markdown_v2(str(e))}"

    details = (
        f"📌 *Group Information* 📌\n\n"
        f"ID: `{chat.id}`\n"
        f"Title: {title}\n"
        f"Type: {escape_markdown_v2(chat.type)}\n"
        f"Username: {username}\n"
        f"Members Count: {member_count_str}\n\n"
        f"👮 *Admins:* \n"
    )

    try:
        admins = await context.bot.get_chat_administrators(chat.id)
        for admin in admins:
            user = admin.user
            role = "Owner" if admin.status == "creator" else "Admin"
            if user.username:
                uname = f"@{escape_markdown_v2(user.username)}"
            else:
                uname = escape_markdown_v2(user.full_name)
            details += f" \\- {uname} \\({escape_markdown_v2(role)}\\)\n"
    except Exception as e:
        details += f"❌ Could not fetch admins \\({escape_markdown_v2(str(e))}\\)"

    await update.message.reply_text(details, parse_mode="MarkdownV2")

# Main entry point
def main():
    db.initialize_database()
    if not BOT_TOKEN or not GROUP_CHAT_ID:
        raise ValueError("BOT_TOKEN or GROUP_CHAT_ID is not set in environment variables.")
    
    # Validate admin IDs
    admin_ids = get_admin_ids()
    if not admin_ids:
        raise ValueError("ADMIN_USER_IDS is not set or empty in environment variables.")
    
    logger.info(f"Bot starting with {len(admin_ids)} admin(s): {', '.join(admin_ids)}")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Register handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("groupinfo", groupinfo))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(PollAnswerHandler(handle_poll_answer))

    # Set up webhook or polling
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
