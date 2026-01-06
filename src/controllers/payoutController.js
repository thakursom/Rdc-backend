const XLSX = require("xlsx");
const User = require("../models/userModel");
const PaymentHistory = require("../models/paymentHistoryModel");
const LogService = require("../services/logService");


class payoutController {

    constructor() { }

    // createPayout method
    async createPayout(req, res, next) {
        try {
            const { userId, email } = req.user;
            const {
                user_id,
                paymentMethod,
                amount,
                description,
                paymentDetails: inputDetails
            } = req.body;

            if (!user_id || !paymentMethod || !amount) {
                return res.status(400).json({
                    success: false,
                    message: "Missing required fields"
                });
            }

            let paymentDetails = {};

            if (paymentMethod === "bank") {
                paymentDetails = {
                    bankName: inputDetails?.bankName || "",
                    accountHolderName: inputDetails?.accountHolderName || "",
                    accountNumber: inputDetails?.accountNumber || "",
                    ifsc: inputDetails?.ifsc || "",
                    swiftCode: inputDetails?.swiftCode || "",
                    branch: inputDetails?.branch || ""
                };
            }

            if (paymentMethod === "paypal") {
                paymentDetails = {
                    paypalEmail: inputDetails?.paypalEmail || ""
                };
            }

            if (paymentMethod === "upi") {
                paymentDetails = {
                    upiId: inputDetails?.upiId || ""
                };
            }

            const payload = {
                user_id,
                paymentMethod,
                amount,
                totalAmount: null,
                description: description || "",
                paymentDetails
            };

            const newPayout = await PaymentHistory.create(payload);

            await LogService.createLog({
                user_id: userId,
                email,
                action: "ADD_PAYMENT_HISTORY_DETAILS",
                description: "Payment History created successfully",
                newData: newPayout,
                req
            });

            return res.status(200).json({
                success: true,
                message: "Payment History created successfully",
                data: newPayout
            });

        } catch (error) {
            console.error("Create Payment History Error:", error);
            next(error);
        }
    }

    // getAllPayouts method
    async getAllPayouts(req, res, next) {
        try {
            const { role, userId } = req.user;
            let { page = 1, limit = 10, search = "" } = req.query;

            page = Number(page);
            limit = Number(limit);

            let query = {};

            if (role !== "Super Admin" && role !== "Manager") {
                const users = await User.find({ parent_id: userId }, { id: 1 });
                const childIds = users.map(u => u.id);
                // childIds.push(userId);
                query.user_id = { $in: childIds };
            }

            // Search filter
            if (search) {
                query.$or = [
                    { paymentMethod: { $regex: search, $options: "i" } },
                    { description: { $regex: search, $options: "i" } },
                    { amount: isNaN(search) ? undefined : Number(search) },
                    { totalAmount: isNaN(search) ? undefined : Number(search) }
                ].filter(Boolean); // remove undefined
            }

            // Count
            const total = await PaymentHistory.countDocuments(query);

            // Aggregation pipeline
            const data = await PaymentHistory.aggregate([
                { $match: query },

                {
                    $lookup: {
                        from: "users",
                        localField: "user_id",
                        foreignField: "id",
                        as: "user"
                    }
                },
                {
                    $unwind: {
                        path: "$user",
                        preserveNullAndEmptyArrays: true
                    }
                },

                { $sort: { createdAt: -1 } },
                { $skip: (page - 1) * limit },
                { $limit: limit },

                {
                    $project: {
                        _id: 1,
                        user_id: 1,
                        amount: 1,
                        totalAmount: 1,
                        paymentMethod: 1,
                        description: 1,
                        paymentDetails: 1,
                        createdAt: 1,
                        userName: "$user.name",
                        userEmail: "$user.email"
                    }
                }
            ]);

            return res.status(200).json({
                success: true,
                message: "Payment History fetched successfully",
                data,
                pagination: {
                    total,
                    page,
                    limit,
                    totalPages: Math.ceil(total / limit),
                },
            });

        } catch (error) {
            console.error("Fetch Payment History Error:", error);
            return res.status(500).json({
                success: false,
                message: error.message || "Internal server error",
            });
        }
    }

    // uploadBulkPayout method
    async uploadBulkPayout(req, res, next) {
        try {
            const { userId, email } = req.user;

            if (!req.file) {
                return res.status(400).json({
                    success: false,
                    message: "No file uploaded"
                });
            }

            const workbook = XLSX.readFile(req.file.path);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const rows = XLSX.utils.sheet_to_json(sheet);

            if (!rows.length) {
                return res.status(400).json({
                    success: false,
                    message: "Excel file is empty"
                });
            }

            /* ----------------------------------
               1️⃣ Collect unique user names
            ---------------------------------- */
            const userNames = [
                ...new Set(
                    rows
                        .map(r => r["User Name"]?.trim())
                        .filter(Boolean)
                )
            ];

            /* ----------------------------------
               2️⃣ Fetch users in ONE query
            ---------------------------------- */
            const users = await User.find(
                { name: { $in: userNames } },
                { id: 1, name: 1 }
            ).lean();

            /* ----------------------------------
               3️⃣ Create map: name → id
            ---------------------------------- */
            const userNameToIdMap = {};
            users.forEach(u => {
                userNameToIdMap[u.name] = u.id;
            });

            const payouts = [];
            const skippedUsers = [];

            /* ----------------------------------
               4️⃣ Build payouts
            ---------------------------------- */
            for (const row of rows) {
                const userName = row["User Name"]?.trim();
                const paymentMethod = row["Payment Method"]?.toLowerCase();

                if (!userName || !paymentMethod || !row["Amount"]) continue;

                const user_id = userNameToIdMap[userName];

                // Skip if user not found
                if (!user_id) {
                    skippedUsers.push(userName);
                    continue;
                }

                let paymentDetails = {};

                if (paymentMethod === "bank") {
                    paymentDetails = {
                        bankName: row["Bank Name"] || "",
                        accountHolderName: row["Account Holder Name"] || "",
                        accountNumber: row["Account Number"] || "",
                        ifsc: row["IFSC/Routing"] || "",
                        swiftCode: row["SWIFT Code"] || "",
                        branch: row["Branch"] || ""
                    };
                }

                if (paymentMethod === "paypal") {
                    paymentDetails = {
                        paypalEmail: row["PayPal Email"] || ""
                    };
                }

                if (paymentMethod === "upi") {
                    paymentDetails = {
                        upiId: row["UPI ID"] || ""
                    };
                }

                payouts.push({
                    user_id,
                    paymentMethod,
                    amount: Number(row["Amount"]),
                    totalAmount: null,
                    description: row["Description"] || "",
                    paymentDetails
                });
            }

            if (!payouts.length) {
                return res.status(400).json({
                    success: false,
                    message: "No valid payout records found",
                    skippedUsers
                });
            }

            /* ----------------------------------
               5️⃣ Bulk insert
            ---------------------------------- */
            const insertedPayouts = await PaymentHistory.insertMany(payouts);

            /* ----------------------------------
               6️⃣ Log
            ---------------------------------- */
            await LogService.createLog({
                user_id: userId,
                email,
                action: "ADD_BULK_PAYOUT",
                description: "Bulk payout uploaded successfully",
                newData: insertedPayouts,
                req
            });

            return res.status(200).json({
                success: true,
                message: "Bulk payout uploaded successfully",
                inserted: insertedPayouts.length,
                skippedUsers // helpful for UI
            });

        } catch (error) {
            console.error("Bulk Payout Upload Error:", error);
            next(error);
        }
    }


}

module.exports = new payoutController();