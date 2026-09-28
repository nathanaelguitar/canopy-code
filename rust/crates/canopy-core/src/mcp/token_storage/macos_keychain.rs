use super::base::{BaseTokenStorage, TokenStorageError};
use super::types::{OAuthCredentials, SecretStorage, TokenStorage};
use indexmap::IndexMap;
use security_framework::item::{ItemClass, ItemSearchOptions, Limit, SearchResult};
use security_framework::passwords::{delete_generic_password, set_generic_password};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const KEYCHAIN_TEST_PREFIX: &str = "__keychain_test__";
const SECRET_PREFIX: &str = "__secret__";
const ERR_SEC_ITEM_NOT_FOUND: i32 = -25300;

/// macOS Keychain storage matching Keytar's generic-password service/account
/// mapping. This intentionally uses the Security Framework directly so it can
/// enumerate all credentials for a service, as Keytar's `findCredentials`
/// does.
#[derive(Clone, Debug)]
pub struct KeychainTokenStorage {
    service_name: String,
}

impl KeychainTokenStorage {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
        }
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    fn is_available_blocking(&self) -> bool {
        let account = format!("{KEYCHAIN_TEST_PREFIX}{}", Uuid::new_v4().simple());
        if set_generic_password(&self.service_name, &account, b"test").is_err() {
            return false;
        }
        let retrieved = generic_password_for(&self.service_name, &account);
        let deleted = delete_generic_password(&self.service_name, &account).is_ok();
        deleted && matches!(retrieved, Ok(value) if value == b"test")
    }

    fn enumerate_accounts(&self) -> Result<Vec<String>, TokenStorageError> {
        let mut options = ItemSearchOptions::new();
        options
            .class(ItemClass::generic_password())
            .service(&self.service_name)
            .limit(Limit::All)
            .load_attributes(true);
        let results = match options.search() {
            Ok(results) => results,
            Err(error) if is_item_not_found(&error) => return Ok(Vec::new()),
            Err(error) => return Err(keychain_error("list keychain credentials", error)),
        };

        Ok(results.iter().filter_map(account_from_result).collect())
    }

    fn get_credentials_blocking(
        &self,
        server_name: &str,
    ) -> Result<Option<OAuthCredentials>, TokenStorageError> {
        let account = BaseTokenStorage::sanitize_server_name(server_name);
        let Some(data) = optional_password(&self.service_name, &account)? else {
            return Ok(None);
        };
        let credentials: OAuthCredentials = serde_json::from_slice(&data).map_err(|_| {
            TokenStorageError::Backend(format!(
                "Failed to parse stored credentials for {server_name}"
            ))
        })?;
        if BaseTokenStorage::is_token_expired(&credentials) {
            return Ok(None);
        }
        Ok(Some(credentials))
    }

    fn set_credentials_blocking(
        &self,
        mut credentials: OAuthCredentials,
    ) -> Result<(), TokenStorageError> {
        BaseTokenStorage::validate_credentials(&credentials)?;
        credentials.updated_at = now_unix_millis();
        let account = BaseTokenStorage::sanitize_server_name(&credentials.server_name);
        let data = serde_json::to_vec(&credentials)?;
        set_generic_password(&self.service_name, &account, &data)
            .map_err(|error| keychain_error("store OAuth credentials", error))
    }

    fn delete_credentials_blocking(&self, server_name: &str) -> Result<(), TokenStorageError> {
        let account = BaseTokenStorage::sanitize_server_name(server_name);
        match delete_generic_password(&self.service_name, &account) {
            Ok(()) => Ok(()),
            Err(error) if is_item_not_found(&error) => Err(TokenStorageError::CredentialsNotFound(
                server_name.to_owned(),
            )),
            Err(error) => Err(keychain_error("delete OAuth credentials", error)),
        }
    }

    fn get_all_credentials_blocking(
        &self,
    ) -> Result<IndexMap<String, OAuthCredentials>, TokenStorageError> {
        let Ok(accounts) = self.enumerate_accounts() else {
            // Keytar logs enumeration failures and returns an empty map.
            return Ok(IndexMap::new());
        };
        let mut credentials = IndexMap::new();
        for account in accounts {
            if account.starts_with(KEYCHAIN_TEST_PREFIX) || account.starts_with(SECRET_PREFIX) {
                continue;
            }
            let Ok(Some(data)) = optional_password(&self.service_name, &account) else {
                continue;
            };
            let Ok(value) = serde_json::from_slice::<OAuthCredentials>(&data) else {
                continue;
            };
            if !BaseTokenStorage::is_token_expired(&value) {
                credentials.insert(account, value);
            }
        }
        Ok(credentials)
    }

    fn clear_all_blocking(&self) -> Result<(), TokenStorageError> {
        let accounts = self.enumerate_accounts().map_err(|error| match error {
            TokenStorageError::Backend(message) => TokenStorageError::Backend(format!(
                "Failed to list servers for clearing: {message}"
            )),
            other => other,
        })?;
        let mut errors = Vec::new();
        for account in accounts {
            if let Err(error) = self.delete_credentials_blocking(&account) {
                errors.push(error.to_string());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(TokenStorageError::Backend(format!(
                "Failed to clear some credentials: {}",
                errors.join(", ")
            )))
        }
    }

    fn set_secret_blocking(&self, key: &str, value: &str) -> Result<(), TokenStorageError> {
        let account = format!("{SECRET_PREFIX}{key}");
        set_generic_password(&self.service_name, &account, value.as_bytes())
            .map_err(|error| keychain_error("store secret", error))
    }

    fn get_secret_blocking(&self, key: &str) -> Result<Option<String>, TokenStorageError> {
        let account = format!("{SECRET_PREFIX}{key}");
        let Some(value) = optional_password(&self.service_name, &account)? else {
            return Ok(None);
        };
        String::from_utf8(value).map(Some).map_err(|error| {
            TokenStorageError::Backend(format!("Stored secret is not UTF-8: {error}"))
        })
    }

    fn delete_secret_blocking(&self, key: &str) -> Result<(), TokenStorageError> {
        let account = format!("{SECRET_PREFIX}{key}");
        match delete_generic_password(&self.service_name, &account) {
            Ok(()) => Ok(()),
            Err(error) if is_item_not_found(&error) => {
                Err(TokenStorageError::SecretNotFound(key.to_owned()))
            }
            Err(error) => Err(keychain_error("delete secret", error)),
        }
    }

    fn list_secrets_blocking(&self) -> Vec<String> {
        self.enumerate_accounts()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|account| account.strip_prefix(SECRET_PREFIX).map(str::to_owned))
            .collect()
    }
}

impl TokenStorage for KeychainTokenStorage {
    async fn get_credentials(
        &self,
        server_name: &str,
    ) -> Result<Option<OAuthCredentials>, TokenStorageError> {
        let storage = self.clone();
        let server_name = server_name.to_owned();
        run_blocking(move || storage.get_credentials_blocking(&server_name)).await
    }

    async fn set_credentials(
        &self,
        credentials: OAuthCredentials,
    ) -> Result<(), TokenStorageError> {
        let storage = self.clone();
        run_blocking(move || storage.set_credentials_blocking(credentials)).await
    }

    async fn delete_credentials(&self, server_name: &str) -> Result<(), TokenStorageError> {
        let storage = self.clone();
        let server_name = server_name.to_owned();
        run_blocking(move || storage.delete_credentials_blocking(&server_name)).await
    }

    async fn list_servers(&self) -> Result<Vec<String>, TokenStorageError> {
        let storage = self.clone();
        run_blocking(move || {
            Ok(storage
                .enumerate_accounts()
                .unwrap_or_default()
                .into_iter()
                .filter(|account| {
                    !account.starts_with(KEYCHAIN_TEST_PREFIX)
                        && !account.starts_with(SECRET_PREFIX)
                })
                .collect())
        })
        .await
    }

    async fn get_all_credentials(
        &self,
    ) -> Result<IndexMap<String, OAuthCredentials>, TokenStorageError> {
        let storage = self.clone();
        run_blocking(move || storage.get_all_credentials_blocking()).await
    }

    async fn clear_all(&self) -> Result<(), TokenStorageError> {
        let storage = self.clone();
        run_blocking(move || storage.clear_all_blocking()).await
    }
}

impl SecretStorage for KeychainTokenStorage {
    async fn is_available(&self) -> Result<bool, TokenStorageError> {
        let storage = self.clone();
        run_blocking(move || Ok(storage.is_available_blocking())).await
    }

    async fn set_secret(&self, key: &str, value: &str) -> Result<(), TokenStorageError> {
        let storage = self.clone();
        let key = key.to_owned();
        let value = value.to_owned();
        run_blocking(move || storage.set_secret_blocking(&key, &value)).await
    }

    async fn get_secret(&self, key: &str) -> Result<Option<String>, TokenStorageError> {
        let storage = self.clone();
        let key = key.to_owned();
        run_blocking(move || storage.get_secret_blocking(&key)).await
    }

    async fn delete_secret(&self, key: &str) -> Result<(), TokenStorageError> {
        let storage = self.clone();
        let key = key.to_owned();
        run_blocking(move || storage.delete_secret_blocking(&key)).await
    }

    async fn list_secrets(&self) -> Result<Vec<String>, TokenStorageError> {
        let storage = self.clone();
        run_blocking(move || Ok(storage.list_secrets_blocking())).await
    }
}

async fn run_blocking<T, F>(operation: F) -> Result<T, TokenStorageError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, TokenStorageError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| TokenStorageError::Backend(format!("Keychain worker failed: {error}")))?
}

fn optional_password(service: &str, account: &str) -> Result<Option<Vec<u8>>, TokenStorageError> {
    match generic_password_for(service, account) {
        Ok(value) => Ok(Some(value)),
        Err(error) if is_item_not_found(&error) => Ok(None),
        Err(error) => Err(keychain_error("read keychain credential", error)),
    }
}

fn generic_password_for(
    service: &str,
    account: &str,
) -> Result<Vec<u8>, security_framework::base::Error> {
    security_framework::passwords::generic_password(
        security_framework::passwords::PasswordOptions::new_generic_password(service, account),
    )
}

fn account_from_result(result: &SearchResult) -> Option<String> {
    result.simplify_dict()?.get("acct").cloned()
}

fn is_item_not_found(error: &security_framework::base::Error) -> bool {
    error.code() == ERR_SEC_ITEM_NOT_FOUND
}

fn keychain_error(operation: &str, error: security_framework::base::Error) -> TokenStorageError {
    TokenStorageError::Backend(format!("Could not {operation}: {error}"))
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
