/**
 * Table operations - CREATE, DROP, ALTER, DESCRIBE, STATS
 */

export { handleCreateTable, handleDropTable } from './create-drop';
export { handleAlterTable, handleReshardTable } from './alter';
export { handleShowTables, handleDescribeTable, handleTableStats } from './describe';
export { TableOperationsAPI, createTableOperationsAPI } from './api';
