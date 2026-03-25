const dbName = process.env.MONGODB_DATABASE;
const username = process.env.MONGODB_USER;
const password = process.env.MONGODB_PASSWORD;

if (!username || !password) {
    throw new Error("MONGODB_USER and MONGODB_PASSWORD are required for Mongo init");
}

const appDb = db.getSiblingDB(dbName);
const existingUser = appDb.getUser(username);
if (!existingUser) {
    appDb.createUser({
        user: username,
        pwd: password,
        roles: [{ role: "readWrite", db: dbName }]
    });
}