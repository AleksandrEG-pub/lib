import logging
from database.database_connection import db


def execute_scripts(sql_scripts_file) -> bool:
    with open(sql_scripts_file, 'r') as file:
        sql_commands = ''
        try:
            sql_commands = file.read()
        except FileNotFoundError:
            logging.error(
                f"failed to read init scripts from file {sql_scripts_file}")
            return False
        with db.cursor() as cursor:
            commands = [cmd.strip() for cmd in sql_commands.split(';') if cmd.strip()]
            
            for i, command in enumerate(commands, 1):
                try:
                    cursor.execute(command)
                    if cursor.rowcount > 0:
                        logging.info(f"Updated {cursor.rowcount} rows in command {i}")
                except Exception as cmd_error:
                    logging.error(f"Error executing command {i}: {cmd_error}")
                    logging.debug(f"Problematic command: {command[:200]}...")
                    raise cmd_error
    return True


def execute_script_all(sql_scripts_file) -> bool:
    with open(sql_scripts_file, 'r') as file:
        sql_commands = ''
        try:
            sql_commands = file.read()
        except FileNotFoundError:
            logging.error(
                f"failed to read init scripts from file {sql_scripts_file}")
            return False
        
        # Debug: Print what was read from the file
        logging.debug(f"Read SQL from {sql_scripts_file}: {sql_commands[:200]}...")
        
        with db.cursor() as cursor:
            try:
                cursor.execute(sql_commands)  # Fixed: execute the SQL content, not the filename
                logging.info(f"executed script: {sql_scripts_file}")
            except Exception as cmd_error:
                logging.debug(f"Problematic command: {sql_commands[:400]}...")
                raise cmd_error
    return True
