import db_handling

db_name = "test.db"

def main():
    # Create database
    if not db_handling.create_database(db_name):
        print("Error creating database, check logs for more information")
        exit()
    
    # Check if database connection was successful
    if not db_handling.init_dbconnection(db_name):
        print("Error connecting to database, check logs for more information")
        exit()

    # Close database connection
    db_handling.close_dbconnection()
    return


if __name__ == "__main__":
    main()
