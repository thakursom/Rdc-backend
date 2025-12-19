const User = require("../models/userModel");
const Log = require("../models/logModel");

class LogController {
    constructor() { }

    async getAllLogs(req, res, next) {
        try {
            const { role, userId } = req.user;
            let { page = 1, limit = 10, search = "" } = req.query;

            page = parseInt(page);
            limit = parseInt(limit);

            const filter = {};

            if (role !== "Super Admin" && role !== "Manager") {
                const users = await User.find({ parent_id: userId }, { id: 1 });
                const childIds = users.map(u => u.id);
                // childIds.push(userId);
                filter.user_id = { $in: childIds };
            }

            if (search) {
                filter.$or = [
                    { email: { $regex: search, $options: "i" } },
                    { action: { $regex: search, $options: "i" } },
                    { description: { $regex: search, $options: "i" } },
                    { ip: { $regex: search, $options: "i" } }
                ];
            }

            const skip = (page - 1) * limit;

            // Fetch logs
            const logs = await Log.find(filter)
                .sort({ createdAt: -1 }) // latest first
                .skip(skip)
                .limit(limit);

            const total = await Log.countDocuments(filter);

            return res.status(200).json({
                success: true,
                page,
                limit,
                total,
                totalPages: Math.ceil(total / limit),
                data: logs
            });

        } catch (error) {
            next(error);
        }
    }
}

module.exports = new LogController();
