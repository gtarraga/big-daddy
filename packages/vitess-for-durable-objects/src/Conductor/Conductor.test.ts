import { describe, it, expect } from 'vitest';
import { env } from 'cloudflare:test';
import { createConductor } from './Conductor';

describe('Conductor', () => {
	describe('API design', () => {
		it('should create a conductor with sql method', () => {
			const conductor = createConductor('test-db', env);

			expect(conductor).toBeDefined();
			expect(conductor.sql).toBeDefined();
			expect(typeof conductor.sql).toBe('function');
		});

		it('should accept tagged template literals', async () => {
			const conductor = createConductor('test-db', env);
			const userId = 123;

			// This should compile and execute without errors
			const result = await conductor.sql`SELECT * FROM users WHERE id = ${userId}`;

			expect(result).toBeDefined();
			expect(result).toHaveProperty('rows');
			expect(Array.isArray(result.rows)).toBe(true);
		});

		it('should handle multiple parameters', async () => {
			const conductor = createConductor('test-db', env);
			const name = 'John';
			const age = 25;

			const result = await conductor.sql`
				SELECT * FROM users
				WHERE name = ${name} AND age > ${age}
			`;

			expect(result).toBeDefined();
			expect(result).toHaveProperty('rows');
		});

		it('should handle queries without parameters', async () => {
			const conductor = createConductor('test-db', env);

			const result = await conductor.sql`SELECT * FROM users`;

			expect(result).toBeDefined();
			expect(result).toHaveProperty('rows');
		});
	});

	describe('Query building', () => {
		it('should build parameterized queries correctly', async () => {
			const conductor = createConductor('test-db', env);
			const id = 123;
			const name = "John's";

			// The conductor should properly parameterize these values
			const result = await conductor.sql`
				SELECT * FROM users
				WHERE id = ${id} AND name = ${name}
			`;

			expect(result).toBeDefined();
		});

		it('should handle complex queries', async () => {
			const conductor = createConductor('test-db', env);
			const minAge = 18;
			const status = 'active';
			const limit = 10;

			const result = await conductor.sql`
				SELECT id, name, email
				FROM users
				WHERE age >= ${minAge}
				  AND status = ${status}
				ORDER BY created_at DESC
				LIMIT ${limit}
			`;

			expect(result).toBeDefined();
			expect(result).toHaveProperty('rows');
		});
	});
});
