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


# TODO:
# - Add a function to add transactions to the database
# - Add a function to add assets to the database
# - Add a function to consolidate transactions and assets
# - Add a function to view the consolidated data on a graph
# - Add a function to export the data to a csv file
# - Add a function to import transactions from a csv file
# - Add a function to import assets from a csv file
# - Add a function to import consolidated data from a csv file
# - Add a function to export consolidated data to a csv file
# - Add a function to add a new account to the database
# - Add a function to add a new asset to the database
# - Add a function to add a new transaction to the database
# - Add a function to add a new consolidated data to the database
# - Add a function to add a new graph to the database
# - Add a function to add a new csv file to the database        



