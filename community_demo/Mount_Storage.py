# Databricks notebook source

storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")
mount_point = dbutils.widgets.get("mount_point")

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="dbw-comm-scope", key="sp_accessdata_id"),
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="dbw-comm-scope", key="sp_accessdata_secret"),
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='dbw-comm-scope', key='tenant_id')}/oauth2/token"
}

# Check if the mount point already exists
existing_mounts = [mount.mountPoint for mount in dbutils.fs.mounts()]
if mount_point not in existing_mounts:
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print(f"Mounted {container_name} to {mount_point}")
else:
    print(f"Mount point {mount_point} already exists. Skipping mount.")
