import { createConnection, Connection } from "mysql2/promise";

export class MySQLClient {
  private connection: Connection;

  public async createConnection(): Promise<void> {
    this.connection = await createConnection({
      host: proccess.env.DATABASE_HOST,
      user: proccess.env.DATABASE_USER,
      database: proccess.env.DATABASE_SCHEMA,
      password: proccess.env.DATABASE_PASSWORD,
      port: proccess.env.DATABASE_PORT,
    });
  }

  public async insertScheduledEmailTemplate(
    email: string,
    template: string
  ): Promise<void> {
    if (!this.connection) throw new Error("MySQL Connection not established.");

    await this.connection.execute(`
      INSERT INTO EmailTransacional (email, template, notSentBecauseUserViewedTargetContent, createdAt, scheduledAt)
      VALUES('${email}', '${template}', false, ${this.connection.escape(
      new Date()
    )}, ${this.connection.escape(new Date())});
    `);
  }

  public async getEmailTemplate(
    email: string,
    template: string
  ): Promise<void> {
    if (!this.connection) throw new Error("MySQL Connection not established.");

    const [rows] = await this.connection.execute(`
      SELECT * FROM EmailTransacional 
      WHERE email = "${email}"
      AND template = "${template}"`);

    return rows.length !== 0 ? rows[0] : null;
  }

  public async disconnect(): Promise<void> {
    if (this.connection) await this.connection.end();
  }
}
