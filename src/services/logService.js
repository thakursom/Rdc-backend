const Log = require("../models/logModel");

class LogService {
    static async createLog({ user_id, email, action, description, oldData = {}, newData = {}, req }) {
        try {
            let ip = req?.ip || "";

            // Convert ::ffff:192.168.1.31 → 192.168.1.31
            if (ip.startsWith("::ffff:")) {
                ip = ip.replace("::ffff:", "");
            }

            await Log.create({
                user_id,
                email,
                action,
                description,
                oldData,
                newData,
                ip,
                userAgent: req?.headers["user-agent"]
            });

        } catch (err) {
            console.error("Log Error:", err);
        }
    }
}

module.exports = LogService;

