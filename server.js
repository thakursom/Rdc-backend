require("dotenv").config();
const express = require("express");
const cors = require("cors");
const path = require("path");

const connectDB = require("./src/config/db");
const routes = require("./src/routes/route");
const errorHandler = require("./src/middlewares/errorHandler");

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json()); // Parse JSON
app.use(express.urlencoded({ extended: true })); // Parse form data
app.use(
    "/uploads/revenues",
    express.static(path.join(__dirname, "src/uploads/revenues"))
);

// Serve contracts folder
app.use(
    "/uploads/contracts",
    express.static(path.join(__dirname, "src/uploads/contracts"))
);

// Serve labelSample folder
app.use(
    "/uploads/labelSample",
    express.static(path.join(__dirname, "src/uploads/labelSample"))
);

// Routes
app.get("/", (req, res) => {
    res.send("API is running...");
});

app.use((req, res, next) => {
    console.log("Incoming request:", req.method, req.url);
    next();
});

app.use("/api", routes);

// Global Error Handler
app.use(errorHandler);

// DB & Server
connectDB().then(() => {
    app.listen(PORT, () => {
        console.log(`Server is running on port ${PORT}`);
    });
}).catch((error) => {
    console.error("Failed to connect to MongoDB:", error);
    process.exit(1);
});