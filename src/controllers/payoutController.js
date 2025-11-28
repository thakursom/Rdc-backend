const PaymentHistory = require("../models/paymentHistoryModel");


class payoutController {

    constructor() { }

    // createPayout method
    async createPayout(req, res, next) {
        try {
            const {
                user_id,
                paymentMethod,
                amount,
                totalAmount,
                description,
                paymentDetails
            } = req.body;

            if (!user_id || !paymentMethod || !amount || !totalAmount || !paymentDetails) {
                return res.status(400).json({
                    success: false,
                    message: "Missing required fields"
                });
            }

            const payload = {
                user_id,
                paymentMethod,
                amount,
                totalAmount,
                description,
                paymentDetails
            };

            const newPayout = await PaymentHistory.create(payload);

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
            let { page = 1, limit = 10, search = "" } = req.query;

            page = Number(page);
            limit = Number(limit);

            let query = {};

            // 🔍 Search filter
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


}

module.exports = new payoutController();