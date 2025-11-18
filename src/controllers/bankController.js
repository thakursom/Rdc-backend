const User = require("../models/userModel");
const BankDetail = require("../models/BankDetailModel");
const ResponseService = require("../services/responseService");

class AuthController {

    constructor() { }

    //addBankDetails method
    async addBankDetails(req, res, next) {
        try {
            const {
                user_id,
                paymentMethod,
                bankName,
                accountHolderName,
                accountNumber,
                ifscRouting,
                swiftCode,
                branch,
                paypalEmail,
                upiId
            } = req.body;

            if (!user_id || !paymentMethod) {
                return res.status(400).json({
                    success: false,
                    message: "User ID & Payment Method are required"
                });
            }

            const newDetails = await BankDetail.create({
                user_id,
                paymentMethod,
                bankName,
                accountHolderName,
                accountNumber,
                ifscRouting,
                swiftCode,
                branch,
                paypalEmail,
                upiId
            });

            return res.status(200).json({
                success: true,
                message: "Bank details saved successfully",
                data: newDetails
            });

        } catch (error) {
            next(error);
        }
    }

    //getBankDetails method
    async getBankDetails(req, res, next) {
        try {
            let { page = 1, limit = 10, search = "" } = req.query;
            const { role, userId } = req.user;

            page = Number(page);
            limit = Number(limit);

            const query = search
                ? {
                    $or: [
                        { bankName: { $regex: search, $options: "i" } },
                        { accountHolderName: { $regex: search, $options: "i" } },
                        { paypalEmail: { $regex: search, $options: "i" } },
                        { upiId: { $regex: search, $options: "i" } },
                    ]
                }
                : {};

            // 🔥 NEW LOGIC: If NOT Super Admin → find all child users
            if (role !== "Super Admin") {
                // 1️⃣ Find users where parent_id = logged in user
                const users = await User.find({ parent_id: userId }, { id: 1 });

                // 2️⃣ Extract IDs
                const childIds = users.map(u => u.id);

                // 3️⃣ Also include own ID
                // childIds.push(userId);

                // 4️⃣ Apply condition → user_id IN [...all ids]
                query.user_id = { $in: childIds };
            }

            const total = await BankDetail.countDocuments(query);

            const data = await BankDetail.aggregate([
                { $match: query },

                {
                    $lookup: {
                        from: "users",              // collection name
                        localField: "user_id",      // BankDetail.user_id
                        foreignField: "id",         // User.id (Number)
                        as: "user"
                    }
                },
                {
                    $unwind: {
                        path: "$user",
                        preserveNullAndEmptyArrays: true
                    }
                },

                { $skip: (page - 1) * limit },
                { $limit: limit },

                {
                    $project: {
                        paymentMethod: 1,
                        bankName: 1,
                        accountHolderName: 1,
                        accountNumber: 1,
                        ifscRouting: 1,
                        swiftCode: 1,
                        branch: 1,
                        paypalEmail: 1,
                        upiId: 1,
                        userName: "$user.name",
                        userEmail: "$user.email"
                    }
                }
            ]);

            return ResponseService.success(res, "Bank details fetched", {
                data,
                pagination: {
                    total,
                    page,
                    limit,
                    totalPages: Math.ceil(total / limit)
                }
            });

        } catch (error) {
            next(error);
        }
    }

    //editBankDetails method
    async editBankDetails(req, res, next) {
        try {
            const { id } = req.params;
            const {
                user_id,
                paymentMethod,
                bankName,
                accountHolderName,
                accountNumber,
                ifscRouting,
                swiftCode,
                branch,
                paypalEmail,
                upiId
            } = req.body;

            if (!id) {
                return res.status(400).json({
                    success: false,
                    message: "Bank Detail ID is required"
                });
            }

            if (!user_id || !paymentMethod) {
                return res.status(400).json({
                    success: false,
                    message: "User ID & Payment Method are required"
                });
            }

            const updatedDetail = await BankDetail.findByIdAndUpdate(
                id,
                {
                    user_id,
                    paymentMethod,
                    bankName,
                    accountHolderName,
                    accountNumber,
                    ifscRouting,
                    swiftCode,
                    branch,
                    paypalEmail,
                    upiId
                },
                { new: true } // return updated document
            );

            if (!updatedDetail) {
                return res.status(404).json({
                    success: false,
                    message: "Bank details not found"
                });
            }

            return res.status(200).json({
                success: true,
                message: "Bank details updated successfully",
                data: updatedDetail
            });
        } catch (error) {
            next(error);
        }
    }

    //getBankDetailById method
    async getBankDetailById(req, res, next) {
        try {
            const { id } = req.query;

            if (!id) {
                return res.status(400).json({
                    success: false,
                    message: "Bank Detail ID is required"
                });
            }

            const bankDetail = await BankDetail.findById(id);

            if (!bankDetail) {
                return res.status(404).json({
                    success: false,
                    message: "Bank detail not found"
                });
            }

            // Use 'id' field of User (numeric)
            const user = await User.findOne({ id: bankDetail.user_id }, { name: 1 });

            return ResponseService.success(res, "Bank detail fetched successfully", {
                data: {
                    ...bankDetail.toObject(),
                    userName: user ? user.name : null
                }
            });

        } catch (error) {
            console.error("Get Bank Detail Error:", error);
            next(error);
        }
    }

    //deleteBankDetail method
    async deleteBankDetail(req, res, next) {
        try {
            const { id } = req.params;

            if (!id) {
                return res.status(400).json({
                    success: false,
                    message: "Bank Detail ID is required"
                });
            }

            const deletedDetail = await BankDetail.findByIdAndDelete(id);

            if (!deletedDetail) {
                return res.status(404).json({
                    success: false,
                    message: "Bank detail not found"
                });
            }

            return res.status(200).json({
                success: true,
                message: "Bank detail deleted successfully"
            });

        } catch (error) {
            next(error);
        }
    }


}

module.exports = new AuthController();