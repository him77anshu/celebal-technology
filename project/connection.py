storage_account_name = "mydatalake123"
container_name = "datalake-files"
mnt_point = "/mnt/customers"

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "a1b2c3d4-5678-1234-9abc-87654321d0fe",
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="azure-kv", key="adls-secret"),
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/09876543-21fe-dcba-1234-56789abcde00/oauth2/token"
}

# Mount the ADLS Gen2 filesystem to DBFS at /mnt/customers
dbutils.fs.mount(
  source = "abfss://datalake-files@mydatalake123.dfs.core.windows.net/",
  mount_point = "/mnt/customers",
  extra_configs = configs
)
