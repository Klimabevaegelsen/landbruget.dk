# db.tf | Database Configuration for Mage AI

# Create a PostgreSQL database instance
resource "google_sql_database_instance" "instance" {
    name                = "${var.app_name}-db-instance"
    region              = var.region
    database_version    = "POSTGRES_14"
    deletion_protection = false
    
    settings {
        tier            = "db-f1-micro"
        
        # Configure database flags
        database_flags {
            name  = "max_connections"
            value = "50"
        }
        
        # Set the zone for the database instance
        location_preference {
            zone = "europe-west1-d"
        }
        
        # Configure backups
        backup_configuration {
            enabled = false
            start_time = "20:00"
            backup_retention_settings {
                retained_backups = 7
                retention_unit = "COUNT"
            }
            transaction_log_retention_days = 7
        }
    }
}

# Create the database
resource "google_sql_database" "database" {
    name     = "${var.app_name}-db"
    instance = google_sql_database_instance.instance.name
}

# Create a database user
resource "google_sql_user" "database-user" {
    name     = data.google_secret_manager_secret_version.db_user.secret_data
    instance = google_sql_database_instance.instance.name
    password = data.google_secret_manager_secret_version.db_password.secret_data
}
