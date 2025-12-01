const Log = require("../models/logModel");

class LogController {
    constructor() { }

    async getAllLogs(req, res, next) {
        try {
            let { page = 1, limit = 10, search = "" } = req.query;

            page = parseInt(page);
            limit = parseInt(limit);

            // Build search filter (optional)
            const filter = {};

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
