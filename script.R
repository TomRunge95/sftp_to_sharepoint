
library(httr)
library(AzureAuth)
library(jsonlite)
library(curl)

tenant <- Sys.getenv("TENANT_ID")
app_id <- Sys.getenv("APP_ID")
secret <- Sys.getenv("SECRET_ID")

tok <- get_azure_token(
  resource = "https://graph.microsoft.com/",
  tenant = tenant,
  app = app_id,
  password = secret,
  auth_type = "authorization_code"
)


remote_base_folder <- Sys.getenv("BASE_FOLDER")


sftp_url <- Sys.getenv("SFTP_URL")
h <- new_handle(userpwd = Sys.getenv("DATA_PW"))



access_token <- tok$credentials$access_token

# -------------------------------
# Konfiguration
# -------------------------------
user_id            <- Sys.getenv("EMAIL")
sftp_url           <- Sys.getenv("SFTP_URL")     

h <- new_handle(userpwd = Sys.getenv("DATA_PW"))

# -------------------------------
# Datei-Existenz NUR per Dateiname prüfen
# -------------------------------
file_exists_onedrive <- function(file_name, year_folder) {
  url <- paste0(
    "https://graph.microsoft.com/v1.0/users/", user_id,
    "/drive/root:/", remote_base_folder, "/", year_folder, "/", file_name
  )
  
  res <- GET(
    url,
    add_headers(Authorization = paste("Bearer", access_token))
  )
  
  status_code(res) == 200
}

# -------------------------------
# SFTP → OneDrive (rekursiv, nach Jahr)
# -------------------------------
upload_sftp_to_onedrive_year <- function(remote_dir) {
  
  con <- curl(remote_dir, handle = h)
  listing <- readLines(con)
  close(con)
  
  for (line in listing) {
    parts <- strsplit(line, "\\s+")[[1]]
    name  <- tail(parts, 1)
    
    if (name %in% c(".", "..")) next
    
    is_dir     <- grepl("^d", line)
    remote_path <- paste0(remote_dir, name, ifelse(is_dir, "/", ""))
    
    if (is_dir) {
      # Rekursiv weiter
      upload_sftp_to_onedrive_year(remote_path)
      
    } else if (grepl("\\.zip$", name, ignore.case = TRUE)) {
      
      # Jahr aus Dateiname ziehen (z.B. 01122025 → 2025)
      year <- sub(".*(\\d{4})\\.zip$", "\\1", name)
      if (!grepl("^\\d{4}$", year)) year <- "UNKNOWN"
      
      # Existenzprüfung
      if (file_exists_onedrive(name, year)) {
        cat("Überspringe (bereits vorhanden):", name, "\n")
        next
      }
      
      cat("Hochladen:", remote_path, "→", year, "/", name, "\n")
      
      # Datei vom SFTP in RAM laden
      tf <- curl_fetch_memory(remote_path, handle = h)
      
      # Upload-URL (mit user_id!)
      upload_url <- paste0(
        "https://graph.microsoft.com/v1.0/users/", user_id,
        "/drive/root:/", remote_base_folder, "/", year, "/", name, ":/content"
      )
      
      res <- PUT(
        url = upload_url,
        add_headers(Authorization = paste("Bearer", access_token)),
        body = tf$content
      )
      
      upload_result <- content(res, as = "parsed")
      
      if (!is.null(upload_result$error)) {
        cat("❌ Fehler:", upload_result$error$message, "\n")
      } else {
        cat("✅ Erfolgreich:", upload_result$name, "\n")
      }
    }
  }
}

# -------------------------------
# Start
# -------------------------------
upload_sftp_to_onedrive_year(sftp_url)

