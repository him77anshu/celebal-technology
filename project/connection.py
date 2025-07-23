storage_account_name = "mydatalake123"
container_name = "datalake-files"
mnt_point = "/mnt/customers"

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "a1b2c3d4-5678-1234-9abc-87654321d0fe",  # your client id
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="azure-kv", key="adls-secret"),  # your secret from secret scope
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/09876543-21fe-dcba-1234-56789abcde00/oauth2/token"
}

# Mount the storage if not mounted yet
if not any(mount.mountPoint == mnt_point for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = mnt_point,
    extra_configs = configs
  )

