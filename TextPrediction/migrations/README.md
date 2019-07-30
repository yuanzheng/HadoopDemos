# Database Migrations #
Database management can be a challenge for projects with multiple developers and multiple environments. The database migration pattern has been a valuable approach for developers.

**Here's a basic overview of the migration approach**

* Each time you want to make a change to the database you build a new Migration file to execute the change (in our case it's a PHP file)
* Each Migration has two functions
    * Up() - which adds the change to the database, e.g. add table, add column, add data, remove uneeded table
    * Down() - which rolls back the change that's made in the Up() function
* The database has a table that keeps track of which Migrations have been applied to the database (in our case the table is call migrations)
* A command line script is used to apply migrations to the database
* You never make changes directly in the database
* You don't make changes to Migrations, you just add new Migrations

**Software**

* For HadoopDemo we are using a PHP migration software call Phinx (Please refer to https://phinx.org/ or https://phinx.readthedocs.io/en/latest/install.html)
* Developers should install PHP 5.5 or later and Phinx
We have a custom bash script we are using to interact with Phinx


**Phinx in our git repos**

In the top-level migrations folder in a git repos, there is an executable bash script called migrate, which you use to execute migration commands,
and one file called migration.template, which is the file used to create migration files.


Example directory structure

TextPrediction /
* migrations /
    * migration.template
    * migrate
    * textprediction /
        * phinx.yml
        * 20190709164429_create_ngram_tables.php
    
**Commands**

The following commands are available for migrations:

    * Status
        * migrate status <domain> -e <environment>
        * example: migrate status textprediction -e local
        * Returns the status of each migration file against the environment database
    * Create
        * migrate create <MigrationName> <domain>
        * example: migrate create MyTable textprediction
        * Creates a migration file in the textprediction subdirectory
    * Migrate
        * migrate <domain> -e <environment>
        * example: migrate textprediction -e local
        * Runs unapplied migrations in the environment database
    * Rollback
        * migrate rollback <domain> -e <environment>
        * example: migrate rollback textprediction -e local
        * Unapplies the last migration in the environment database
        

