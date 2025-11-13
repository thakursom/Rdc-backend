const bcrypt = require("bcryptjs");
const User = require("../models/userModel");
const ResponseService = require("../services/responseService");

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

            if (role !== "Super Admin") {
                return ResponseService.error(res, "Access denied. Only Super Admin can view artists.", 403);
            }

            const skip = (page - 1) * limit;
            let query = {};

            // Role filter
            if (filterRole === "label") {
                query.role = "Label";
            } else if (filterRole === "manager") {
                query.role = { $in: ["Super Admin", "Manager"] };
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
            const { userId } = req.user;
            let {
                name,
                email,
                password,
                role,
            } = req.body;

            if (!name || !email || !password || !role) {
                return res.status(400).json({
                    success: false,
                    message: "name, email, password & role are required"
                });
            }

            // ✅ Check existing email
            const existingUser = await User.findOne({ email });
            if (existingUser) {
                return res.status(400).json({
                    success: false,
                    message: "Email already exists"
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
                email,
                password: hashedPassword,
                role,
                parent_id: userId || null
            });

            await newUser.save();

            return res.status(200).json({
                success: true,
                message: "User added successfully",
                data: newUser
            });

        } catch (error) {
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
                .select("_id id name parent_id");

            return ResponseService.success(res, "Label fetched successfully", { labels });

        } catch (error) {
            next(error);
        }
    }


}

module.exports = new UserController();
