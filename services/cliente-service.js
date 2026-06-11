const oracledb = require('oracledb');

// Ajustes globales
oracledb.outFormat = oracledb.OBJECT;
oracledb.fetchAsString = [oracledb.CLOB]; // no afecta BLOB; mantenido por si lees CLOB en otros queries
oracledb.autoCommit = true;

// === Opción B: forzar almacenamiento en BLOB/CLOB ===
// Usa un nombre NUEVO para garantizar que se aplique el metadata (si la colección ya existe con JSON nativo,
// el metadata se ignora). Cambia 'clientes_blob' si lo deseas o usa la variable de entorno.
const CLIENTES_COLLECTION = process.env.SODA_COLLECTION || 'clientes_blob';

// Metadata para SODA que fuerza BLOB (puedes cambiar a CLOB si prefieres)
const COLLECTION_METADATA_BLOB = {
  contentColumn: { name: 'JSON_DOCUMENT', sqlType: 'BLOB' },
  keyColumn: { name: 'ID', sqlType: 'VARCHAR2', maxLength: 255, assignmentMethod: 'GUID' },
  versionColumn: { name: 'VERSION', method: 'SHA256' },
  lastModifiedColumn: { name: 'LAST_MODIFIED' },
  creationTimeColumn: { name: 'CREATED_ON' }
};

// Helper: crea/abre la colección asegurando el metadata de BLOB (si es nueva)
async function getClientesCollection(connection) {
  const soda = connection.getSodaDatabase();
  // Nota: createCollection crea si no existe; si ya existe con otro storage, el metadata se ignora.
  // Por eso usamos un NOMBRE NUEVO (clientes_blob) para garantizar BLOB.
  return soda.createCollection(CLIENTES_COLLECTION, { metaData: COLLECTION_METADATA_BLOB });
}

module.exports = class ClienteService {
  constructor() {}

  static async init() {
    console.log(`process.env.DB_USER: ${process.env.DB_USER}`);
    console.log(`process.env.DB_PASSWORD: ${process.env.DB_PASSWORD ? '***' : '(vacío)'}`);
    console.log(`process.env.CONNECT_STRING: ${process.env.CONNECT_STRING}`);
    console.log(`SODA collection: ${CLIENTES_COLLECTION}`);

    try {
      console.log('Creando pool de conexiones...');
      await oracledb.createPool({
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        connectString: process.env.CONNECT_STRING,
        // Si usas ADB con wallet y Thick mode, puedes necesitar:
        // sessionCallback: ...,
        // homogeneous: true,
      });
      console.log('Pool de conexiones creado.');
      return new ClienteService();
    } catch (e) {
      console.error('Error en conexion:');
      console.error(e);
      throw e;
    }
  }

  async getAll() {
    let connection;
    const result = [];

    try {
      connection = await oracledb.getConnection();
      const clienteCollection = await getClientesCollection(connection);

      const docs = await clienteCollection.find().getDocuments();
      docs.forEach(d => {
        result.push({
          id: d.key,
          createdOn: d.createdOn,
          lastModified: d.lastModified,
          ...d.getContent(), // SODA convierte automáticamente el BLOB JSON a objeto JS
        });
      });
    } catch (err) {
      console.error('[getAll] ', err);
      throw err;
    } finally {
      if (connection) {
        try { await connection.close(); } catch (err) { console.error(err); }
      }
    }
    return result;
    }

  async getById(clienteId) {
    let connection;
    try {
      connection = await oracledb.getConnection();
      const clientesCollection = await getClientesCollection(connection);

      const doc = await clientesCollection.find().key(clienteId).getOne();
      if (!doc) return null;

      return {
        id: doc.key,
        createdOn: doc.createdOn,
        lastModified: doc.lastModified,
        ...doc.getContent(),
      };
    } catch (err) {
      console.error('[getById] ', err);
      throw err;
    } finally {
      if (connection) {
        try { await connection.close(); } catch (err) { console.error(err); }
      }
    }
  }

  async save(cliente) {
    let connection;
    try {
      connection = await oracledb.getConnection();
      const clientesCollection = await getClientesCollection(connection);

      // insertOneAndGet retorna metadatos del doc insertado
      const inserted = await clientesCollection.insertOneAndGet(cliente);
      return {
        id: inserted.key,
        createdOn: inserted.createdOn,
        lastModified: inserted.lastModified,
      };
    } catch (err) {
      console.error('[save] ', err);
      throw err;
    } finally {
      if (connection) {
        try { await connection.close(); } catch (err) { console.error(err); }
      }
    }
  }

  async update(id, cliente) {
    let connection;
    try {
      connection = await oracledb.getConnection();
      const clienteCollection = await getClientesCollection(connection);

      const updated = await clienteCollection.find().key(id).replaceOneAndGet(cliente);
      if (!updated) return null;

      return {
        id: updated.key,
        createdOn: updated.createdOn,
        lastModified: updated.lastModified,
      };
    } catch (err) {
      console.error('[update] ', err);
      throw err;
    } finally {
      if (connection) {
        try { await connection.close(); } catch (err) { console.error(err); }
      }
    }
  }

  async deleteById(clienteId) {
    let connection;
    try {
      connection = await oracledb.getConnection();
      const clienteCollection = await getClientesCollection(connection);

      // remove() retorna true/false
      return await clienteCollection.find().key(clienteId).remove();
    } catch (err) {
      console.error('[deleteById] ', err);
      throw err;
    } finally {
      if (connection) {
        try { await connection.close(); } catch (err) { console.error(err); }
      }
    }
  }

  async closePool() {
    console.log('Closing connection pool...');
    try {
      await oracledb.getPool().close(10);
      console.log('Pool closed');
    } catch (err) {
      console.error(err);
    }
  }
};
