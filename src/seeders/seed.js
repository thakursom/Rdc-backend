require("dotenv").config();
const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");

const Role = require("../models/roleModel");
const User = require("../models/userModel");
const connectDB = require("../config/db");

const seedData = async () => {
    try {
        await connectDB();

        console.log("Clearing old data...");
        await Role.deleteMany();
        await User.deleteMany();

        console.log("Seeding roles...");
        await Role.create([
            { role: "Super Admin", permissions: ["user:create", "user:update", "user:delete", "user:view"] },
            { role: "Admin", permissions: ["user:create", "user:update", "user:view"] },
            { role: "Manager", permissions: ["user:update", "user:view"] },
            { role: "Label", permissions: ["user:view"] },
            { role: "Sub Label", permissions: ["user:view"] },
        ]);

        console.log("Roles Seeded");

        // const hashedPassword = await bcrypt.hash("123456", 10);

        // await User.create([
        //     {
        //         name: "Super Admin",
        //         email: "superadmin@gmail.com",
        //         password: hashedPassword,
        //         role: "superadmin",
        //     },
        //     {
        //         name: "Admin User",
        //         email: "admin@gmail.com",
        //         password: hashedPassword,
        //         role: "admin",
        //     },
        //     {
        //         name: "Manager User",
        //         email: "manager@gmail.com",
        //         password: hashedPassword,
        //         role: "manager",
        //     },
        //     {
        //         name: "Normal User",
        //         email: "user@gmail.com",
        //         password: hashedPassword,
        //         role: "user",
        //     },
        // ]);

        // console.log("Users Seeded Successfully");
        mongoose.connection.close();
    } catch (err) {
        console.error("Seeder Error:", err);
        mongoose.connection.close();
    }
};

seedData();
