package mssql

import (
	"fmt"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/services/sql/parse"
	"log"

	"github.com/Azure/azure-sdk-for-go/services/preview/sql/mgmt/v3.0/sql"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"

	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/azure"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/clients"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/services/mssql/validate"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/timeouts"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func resourceArmMsSqlSyncGroup() *schema.Resource {
	return &schema.Resource{
		Create: resourceArmMsSqlSyncGroupCreateUpdate,
		Read:   resourceArmMsSqlSyncGroupRead,
		Update: resourceArmMsSqlSyncGroupCreateUpdate,
		Delete: resourceArmMsSqlSyncGroupDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: azure.ValidateMsSqlSyncGroupName,
			},

			"resource_group_name": azure.SchemaResourceGroupName(),

			"server_name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: azure.ValidateMsSqlServerName,
			},

			"database_name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: azure.ValidateMsSqlDatabaseName,
			},

			"conflict_resolution_policy": {
				Type:     schema.TypeString,
				Required: true,
				ValidateFunc: validation.StringInSlice([]string{
					string(sql.HubWin),
					string(sql.MemberWin),
				}, false),
			},

			"interval": {
				Type:     schema.TypeInt,
				Optional: true,
			},

			"sync_database_id": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validate.MsSqlDatabaseID,
			},

			"hub_database_username": {
				Type:     schema.TypeString,
				Required: true,
			},

			"hub_database_password": {
				Type:     schema.TypeString,
				Required: true,
			},

			"primary_sync_member_name": {
				Type:     schema.TypeString,
				Optional: true,
			},

			"table": {
				Type:     schema.TypeList,
				Required: true,
				MinItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:     schema.TypeString,
							Required: true,
							// TODO ValidateFunc for table name
						},

						"column": {
							Type:     schema.TypeList,
							Optional: true,
							MinItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"name": {
										Type:     schema.TypeString,
										Required: true,
										// TODO ValidateFunc for field name
									},

									"data_size": {
										Type:     schema.TypeString,
										Required: true,
									},

									"data_type": {
										Type:     schema.TypeString,
										Required: true,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func resourceArmMsSqlSyncGroupCreateUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).MSSQL.SyncGroupsClient
	ctx, cancel := timeouts.ForCreateUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	name := d.Get("name").(string)
	resourceGroup := d.Get("resource_group_name").(string)
	serverName := d.Get("server_name").(string)
	databaseName := d.Get("database_name").(string)
	syncDatabaseId := d.Get("sync_database_id").(string)
	hubDbUsername := d.Get("hub_database_username").(string)
	hubDbPassword := d.Get("hub_database_password").(string)

	//if d.IsNewResource() {
	//	existing, err := client.Get(ctx, resourceGroup, serverName, databaseName, name)
	//	if err != nil {
	//		if !utils.ResponseWasNotFound(existing.Response) {
	//			return fmt.Errorf("checking for presence of existing SQL Sync Group %q (Resource Group %q, Server %q, Database %q): %+v", name, resourceGroup, serverName, databaseName, err)
	//		}
	//	}
	//
	//	if existing.ID != nil && *existing.ID != "" {
	//		return tf.ImportAsExistsError("azurerm_sql_sync_group", *existing.ID)
	//	}
	//}

	properties := sql.SyncGroup{
		SyncGroupProperties: &sql.SyncGroupProperties{
			ConflictResolutionPolicy: d.Get("conflict_resolution_policy").(sql.SyncConflictResolutionPolicy),
			HubDatabaseUserName:      utils.String(hubDbUsername),
			HubDatabasePassword:      utils.String(hubDbPassword),
			SyncDatabaseID:           utils.String(syncDatabaseId),
			Schema: &sql.SyncGroupSchema{
				Tables: expandMsSqlSyncGroupSchemaTables(d.Get("tables").([]interface{})),
			},
		},
	}

	if interval, exists := d.GetOk("interval"); exists {
		properties.SyncGroupProperties.Interval = utils.Int32(int32(interval.(int)))
	}

	if primarySyncMemberName, exists := d.GetOk("primary_sync_member_name"); exists {
		properties.SyncGroupProperties.Schema.MasterSyncMemberName = utils.String(primarySyncMemberName.(string))
	}

	future, err := client.CreateOrUpdate(ctx, resourceGroup, serverName, databaseName, name, properties)
	if err != nil {
		return fmt.Errorf("creating/updating SQL Sync Group %q (Resource Group %q, Server %q, Database: %q): %+v", name, resourceGroup, serverName, databaseName, err)
	}

	if err = future.WaitForCompletionRef(ctx, client.Client); err != nil {
		return fmt.Errorf("waiting on create/update operation for SQL Sync Group %q (Resource Group %q, Server %q, Database: %q): %+v", name, resourceGroup, serverName, databaseName, err)
	}

	return resourceArmMsSqlSyncGroupRead(d, meta)
}

func resourceArmMsSqlSyncGroupRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).MSSQL.SyncGroupsClient
	ctx, cancel := timeouts.ForCreateUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.SqlSyncGroupID(d.Id())
	if err != nil {
		return err
	}

	resp, err := client.Get(ctx, id.ResourceGroup, id.ServerName, id.DatabaseName, id.Name)
	if err != nil {
		if utils.ResponseWasNotFound(resp.Response) {
			log.Printf("[INFO] Error reading SQL Sync Group %q - removing from state", d.Id())
			d.SetId("")
			return nil
		}

		return fmt.Errorf("reading SQL Sync Group %q (Resource Group %q, Server %q, Database: %q): %+v", id.Name, id.ResourceGroup, id.ServerName, id.DatabaseName, err)
	}

	if err = d.Set("name", resp.Name); err != nil {
		return fmt.Errorf("setting %q", "name")
	}

	if err = d.Set("resource_group_name", id.ResourceGroup); err != nil {
		return fmt.Errorf("setting %q", "resource_group_name")
	}

	if err = d.Set("server_name", id.ServerName); err != nil {
		return fmt.Errorf("setting %q", "server_name")
	}

	if err = d.Set("database_name", id.DatabaseName); err != nil {
		return fmt.Errorf("setting %q", "database_name")
	}

	if err = d.Set("conflict_resolution_policy", resp.ConflictResolutionPolicy); err != nil {
		return fmt.Errorf("setting %q", "conflict_resolution_policy")
	}

	if resp.Interval == nil {
		return fmt.Errorf("interval returned was null for SQL Sync Group %q (Resource Group %q, Server %q, Database: %q)", id.Name, id.ResourceGroup, id.ServerName, id.DatabaseName)
	} else if err = d.Set("interval", int(*resp.Interval)); err != nil {
		return fmt.Errorf("setting %q", "interval")
	}

	if err = d.Set("sync_database_id", resp.SyncDatabaseID); err != nil {
		return fmt.Errorf("setting %q", "sync_database_id")
	}

	if err = d.Set("hub_database_username", resp.HubDatabaseUserName); err != nil {
		return fmt.Errorf("setting %q", "hub_database_username")
	}

	if err = d.Set("hub_database_password", resp.HubDatabasePassword); err != nil {
		return fmt.Errorf("setting %q", "hub_database_password")
	}

	if resp.Schema != nil {
		if err = d.Set("primary_sync_member_name", resp.Schema.MasterSyncMemberName); err != nil {
			return fmt.Errorf("setting %q", "primary_sync_member_name")
		}

		if err = d.Set("table", flattenMsSqlSyncGroupSchemaTables(resp.Schema.Tables)); err != nil {
			return fmt.Errorf("setting %q", "table")
		}
	}

	return nil
}

func resourceArmMsSqlSyncGroupDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).Sql.SyncGroupsClient
	ctx, cancel := timeouts.ForDelete(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.SqlSyncGroupID(d.Id())
	if err != nil {
		return err
	}

	future, err := client.Delete(ctx, id.ResourceGroup, id.ServerName, id.DatabaseName, id.Name)
	if err != nil {
		return fmt.Errorf("deleting SQL Sync Group %q (Resource Group %q, Server %q, Database: %q): %+v", id.Name, id.ResourceGroup, id.ServerName, id.DatabaseName, err)
	}

	return future.WaitForCompletionRef(ctx, client.Client)
}

func flattenMsSqlSyncGroupSchemaTables(tables *[]sql.SyncGroupSchemaTable) []interface{} {
	if tables == nil {
		return []interface{}{}
	}

	out := make([]interface{}, 0, len(*tables))

	for _, table := range *tables {
		name := ""
		if table.QuotedName != nil {
			name = *table.QuotedName
		}

		out = append(out, map[string]interface{}{
			"name":   name,
			"column": flattenMsSqlSyncGroupSchemaTableColumns(table.Columns),
		})
	}
	return out
}

func flattenMsSqlSyncGroupSchemaTableColumns(columns *[]sql.SyncGroupSchemaTableColumn) []interface{} {
	if columns == nil {
		return []interface{}{}
	}

	out := make([]interface{}, 0, len(*columns))

	for _, column := range *columns {
		name := ""
		if column.QuotedName != nil {
			name = *column.QuotedName
		}

		dataSize := ""
		if column.DataSize != nil {
			dataSize = *column.DataSize
		}

		dataType := ""
		if column.DataType != nil {
			dataType = *column.DataType
		}

		out = append(out, map[string]interface{}{
			"name":      name,
			"data_size": dataSize,
			"data_type": dataType,
		})
	}

	return out
}

func expandMsSqlSyncGroupSchemaTables(tables []interface{}) *[]sql.SyncGroupSchemaTable {
	out := make([]sql.SyncGroupSchemaTable, 0, len(tables))

	for _, tableRaw := range tables {
		table := tableRaw.(map[string]interface{})
		outTable := sql.SyncGroupSchemaTable{
			Columns:    expandMsSqlSyncGroupSchemaTableColumns(table["columns"].([]interface{})),
			QuotedName: utils.String(table["name"].(string)),
		}
		out = append(out, outTable)
	}
	return &out
}

func expandMsSqlSyncGroupSchemaTableColumns(columns []interface{}) *[]sql.SyncGroupSchemaTableColumn {
	out := make([]sql.SyncGroupSchemaTableColumn, 0, len(columns))

	for _, columnRaw := range columns {
		column := columnRaw.(map[string]interface{})
		outColumn := sql.SyncGroupSchemaTableColumn{
			QuotedName: utils.String(column["name"].(string)),
			DataSize:   utils.String(column["data_size"].(string)),
			DataType:   utils.String(column["data_type"].(string)),
		}
		out = append(out, outColumn)
	}
	return &out
}
