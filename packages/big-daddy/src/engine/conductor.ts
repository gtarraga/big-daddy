/**
 * Conductor - Re-export from modularized conductor module
 *
 * This file provides backward compatibility by re-exporting all conductor functionality
 * from the new modularized conductor/ directory.
 */

export {
	ConductorClient,
	createConductor,
} from './conductor/index';

export type {
	QueryResult,
	ConductorAPI,
} from './conductor/types';

// Export table operations API
export {
	createTableOperationsAPI,
	TableOperationsAPI,
} from './conductor/tables';
