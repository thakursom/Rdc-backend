const XLSX = require("xlsx");

const bcrypt = require("bcryptjs");
const User = require("../models/userModel");
const ResponseService = require("../services/responseService");
const LogService = require("../services/logService");

class UserController {

    constructor() { }

    //getUsers method
    async getUsers(req, res, next) {
        try {
            const { id } = req.user;
            const users = await User.find({ _id: id });
            if (!users.length) throw { statusCode: 404, message: "No users found" };
            return ResponseService.success(res, "Users fetched successfully", users);
        } catch (error) {
            next(error);
        }
    }

    //getAllUsers method
    async getAllUsers(req, res, next) {
        try {
            const { role } = req.user;

            const {
                page = 1,
                limit = 20,
                filterRole,
                search
            } = req.query;

            // if (role !== "Super Admin") {
            //     return ResponseService.error(res, "Access denied. Only Super Admin can view artists.", 403);
            // }

            const skip = (page - 1) * limit;
            let query = {};

            // Role filter
            if (filterRole === "label") {
                query.role = "Label";
            } else if (filterRole === "manager") {
                query.role = { $in: ["Super Admin", "Manager"] };
            } else if (filterRole === "sub label") {
                query.role = "Sub Label";
            }

            // Search filter (name, email, role)
            if (search && search.trim() !== "") {
                const regex = new RegExp(search, "i");
                query.$or = [
                    { name: regex },
                    { email: regex },
                    { role: regex }
                ];
            }


            const users = await User.find(query)
                // .sort({ createdAt: -1 })
                .skip(skip)
                .limit(Number(limit));

            const total = await User.countDocuments(query);

            return ResponseService.success(res, "All users fetched successfully", {
                users,
                pagination: {
                    total,
                    page: Number(page),
                    limit: Number(limit),
                    totalPages: Math.ceil(total / limit)
                }
            });

        } catch (error) {
            next(error);
        }
    }

    //fetchSubLabel method
    async fetchSubLabel(req, res, next) {
        try {
            const { parent_id } = req.query;

            if (!parent_id) {
                return ResponseService.error(res, "parent_id is required.", 400);
            }

            const labels = await User.find({ parent_id });

            return ResponseService.success(res, "Sub Label fetched successfully", { labels });

        } catch (error) {
            next(error);
        }
    }

    //addUser method
    async addUser(req, res, next) {
        try {
            const { userId, email } = req.user;
            let {
                name,
                userEmail,
                password,
                role,
            } = req.body;

            if (!name || !userEmail || !password || !role) {
                return res.status(400).json({
                    success: false,
                    message: "name, userEmail, password & role are required"
                });
            }

            // ✅ Check existing userEmail
            const existingUser = await User.findOne({ email: userEmail });
            if (existingUser) {
                return res.status(400).json({
                    success: false,
                    message: "userEmail already exists"
                });
            }

            // ✅ Auto-increment ID
            const lastUser = await User.findOne().sort({ id: -1 });
            const newId = lastUser ? lastUser.id + 1 : 1;

            // ✅ Hash password
            const hashedPassword = await bcrypt.hash(password, 10);


            const newUser = new User({
                id: newId,
                name,
                email: userEmail,
                password: hashedPassword,
                role,
                parent_id: userId || null
            });

            await newUser.save();

            await LogService.createLog({
                user_id: userId,
                email: email,
                action: `ADDED_NEW_USER`,
                description: "User added successfully",
                newData: newUser,
                req
            });

            return res.status(200).json({
                success: true,
                message: "User added successfully",
                data: newUser
            });

        } catch (error) {
            console.log(error);

            return res.status(500).json({
                success: false,
                message: error.message
            });
        }
    }

    //fetchAllLabels method
    async fetchAllLabels(req, res, next) {
        try {
            const { search } = req.query;

            let query = { role: "Label" };

            // ✅ optional search filter
            if (search) {
                query.name = { $regex: search, $options: "i" };
            }

            const labels = await User.find(query)
                .select("_id id name parent_id amount");

            return ResponseService.success(res, "Label fetched successfully", { labels });

        } catch (error) {
            next(error);
        }
    }

    //fetchAllSubLabel method
    async fetchAllSubLabel(req, res, next) {
        try {

            const { userId } = req.user;

            const { search } = req.query;

            let query = { parent_id: userId };

            if (search) {
                query.name = { $regex: search, $options: "i" };
            }

            const labels = await User.find(query)
                .select("_id id name parent_id");

            return ResponseService.success(res, "Labels fetched successfully", { labels });

        } catch (error) {
            next(error);
        }
    }

    async uploadLabelAsUser(req, res, next) {
        try {
            const { userId, email } = req.user;

            if (!req.file) {
                return res.status(400).json({ error: "No file uploaded" });
            }

            // Read Excel
            const workbook = XLSX.readFile(req.file.path);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const jsonData = XLSX.utils.sheet_to_json(sheet);

            if (jsonData.length === 0) {
                return res.status(400).json({ error: "Excel file is empty" });
            }

            // Get last used ID
            const lastUser = await User.findOne().sort({ id: -1 });
            let nextId = lastUser ? lastUser.id + 1 : 1;

            const mappedUsers = [];

            for (const r of jsonData) {
                // Skip empty rows
                if (!r.email && !r.name) continue;

                mappedUsers.push({
                    id: nextId++,
                    third_party_id: r.third_party_id || null,
                    third_party_sub_id: r.third_party_sub_id || null,
                    third_party_username: r.third_party_username || null,
                    access_token: r.access_token || null,
                    parent_id: r.parent_id || null,
                    name: r.name || null,
                    email: r.email || null,
                    phone: r.phone || null,
                    country_id: r.country_id || null,
                    email_verified_at: r.email_verified_at || null,
                    password: r.password || null,
                    remember_token: r.remember_token || null,
                    role: r.role || null,
                    amount: r.amount || null,
                });
            }

            // Insert into User collection
            await User.insertMany(mappedUsers);

            // Log
            await LogService.createLog({
                user_id: userId,
                email,
                action: "ADD_BULK_LABEL",
                description: "Label uploaded successfully from Excel",
                newData: mappedUsers,
                req
            });

            return res.json({
                success: true,
                message: "Label uploaded and processed successfully",
                inserted: mappedUsers.length
            });

        } catch (error) {
            next(error);
        }
    }

}

module.exports = new UserController();
