/**
 * Centralized logger for Big Daddy database system
 *
 * Uses hatchlet for structured logging with correlation IDs
 * and tag-based context tracking across distributed Durable Objects.
 */

import { Logger } from "hatchlet";

/**
 * Global logger instance for Big Daddy
 *
 * This logger should be used throughout the application with structured
 * tags to enable effective debugging and analytics.
 */
export const logger = new Logger();
