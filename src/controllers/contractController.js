const path = require("path");
const fs = require("fs");

const User = require("../models/userModel");
const Contract = require("../models/contractModel");
const ContractLog = require("../models/contractLogModel");
const ResponseService = require("../services/responseService");
const sendEmail = require("../utils/sendEmail");
const contractReminderTemplate = require("../utils/emailTemplates/contractReminderTemplate");
const sendWhatsappMessage = require("../utils/messente");



class contractController {

    constructor() { }

    //addContract method
    async addContract(req, res, next) {
        try {
            const {
                label,
                contractName,
                description,
                startDate,
                endDate,
                labelPercentage
            } = req.body;

            if (!req.file) {
                return res.status(400).json({
                    success: false,
                    message: "PDF file is required",
                });
            }

            const newContract = await Contract.create({
                user_id: label,
                contractName,
                description: description || "",
                startDate,
                endDate,
                labelPercentage: labelPercentage || 0,
                pdf: req.file.filename,
            });

            await ContractLog.create({
                user_id: label,
                contract_id: newContract._id,
                action: "add",
                data: {
                    requestBody: req.body,
                    contract: newContract,
                },
                message: `Contract "${contractName}" added.`,
                ipAddress: req.ip,
            });

            return res.status(201).json({
                success: true,
                message: "Contract uploaded successfully",
                data: newContract,
            });
        } catch (error) {
            console.error("Add Contract Error:", error);
            next(error);
        }
    }

    //editContract method
    async editContract(req, res, next) {
        try {
            const { id } = req.params;
            const {
                label,
                contractName,
                description,
                startDate,
                endDate,
                labelPercentage
            } = req.body;

            if (!id) {
                return res.status(400).json({
                    success: false,
                    message: "Contract ID is required",
                });
            }

            if (!label || !contractName) {
                return res.status(400).json({
                    success: false,
                    message: "User ID and Contract Name are required",
                });
            }

            // Find existing contract
            const existingContract = await Contract.findById(id);
            if (!existingContract) {
                return res.status(404).json({
                    success: false,
                    message: "Contract not found",
                });
            }

            // Delete old PDF if new uploaded
            let pdfFile = existingContract.pdf;
            if (req.file) {
                const oldFilePath = path.join(
                    __dirname,
                    "../uploads/contracts",
                    existingContract.pdf
                );
                if (fs.existsSync(oldFilePath)) {
                    fs.unlinkSync(oldFilePath);
                }
                pdfFile = req.file.filename;
            }

            // Update contract
            const updatedContract = await Contract.findByIdAndUpdate(
                id,
                {
                    user_id: label,
                    contractName,
                    description: description || "",
                    startDate,
                    endDate,
                    labelPercentage: labelPercentage || 0,
                    pdf: pdfFile,
                },
                { new: true }
            );

            await ContractLog.create({
                user_id: label,
                contract_id: updatedContract._id,
                action: "update",
                data: {
                    before: existingContract,
                    after: updatedContract,
                },
                message: `Contract "${contractName}" updated.`,
                ipAddress: req.ip,
            });

            return res.status(200).json({
                success: true,
                message: "Contract updated successfully",
                data: updatedContract,
            });
        } catch (error) {
            console.error("Edit Contract Error:", error);
            next(error);
        }
    }

    //getAllContracts method
    // async getAllContracts(req, res, next) {
    //     try {
    //         let { page = 1, limit = 10, search = "" } = req.query;
    //         page = Number(page);
    //         limit = Number(limit);

    //         // ✅ Build search query
    //         const query = search
    //             ? {
    //                 $or: [
    //                     { contractName: { $regex: search, $options: "i" } },
    //                     { label: { $regex: search, $options: "i" } },
    //                 ],
    //             }
    //             : {};

    //         // ✅ Total count for pagination
    //         const total = await Contract.countDocuments(query);

    //         // ✅ Fetch paginated data with user details
    //         const data = await Contract.aggregate([
    //             { $match: query },

    //             {
    //                 $lookup: {
    //                     from: "users",           // MongoDB collection name for User
    //                     localField: "user_id",   // Contract.user_id
    //                     foreignField: "id",      // User.id (Number)
    //                     as: "user"
    //                 }
    //             },
    //             {
    //                 $unwind: {
    //                     path: "$user",
    //                     preserveNullAndEmptyArrays: true
    //                 }
    //             },

    //             { $sort: { createdAt: -1 } },
    //             { $skip: (page - 1) * limit },
    //             { $limit: limit },

    //             {
    //                 $project: {
    //                     _id: 1,
    //                     user_id: 1,
    //                     contractName: 1,
    //                     description: 1,
    //                     label: 1,
    //                     startDate: 1,
    //                     endDate: 1,
    //                     pdf: 1,
    //                     status: 1,
    //                     createdAt: 1,
    //                     userName: "$user.name",
    //                     userEmail: "$user.email"
    //                 }
    //             }
    //         ]);

    //         // ✅ Send paginated response
    //         return res.status(200).json({
    //             success: true,
    //             message: "Contracts fetched successfully",
    //             data,
    //             pagination: {
    //                 total,
    //                 page,
    //                 limit,
    //                 totalPages: Math.ceil(total / limit),
    //             },
    //         });

    //     } catch (error) {
    //         console.error("Fetch Contracts Error:", error);
    //         return res.status(500).json({
    //             success: false,
    //             message: error.message || "Internal server error",
    //         });
    //     }
    // }

    async getAllContracts(req, res, next) {
        try {
            let { page = 1, limit = 10, search = "" } = req.query;
            const { role, userId } = req.user;

            page = Number(page);
            limit = Number(limit);

            let query = {};

            // 🔍 Search filter
            if (search) {
                query.$or = [
                    { contractName: { $regex: search, $options: "i" } },
                    { label: { $regex: search, $options: "i" } },
                ];
            }

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

            // Count
            const total = await Contract.countDocuments(query);

            // Aggregation
            const data = await Contract.aggregate([
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
                        contractName: 1,
                        description: 1,
                        label: 1,
                        startDate: 1,
                        endDate: 1,
                        pdf: 1,
                        status: 1,
                        createdAt: 1,
                        userName: "$user.name",
                        userEmail: "$user.email",
                    }
                }
            ]);

            return res.status(200).json({
                success: true,
                message: "Contracts fetched successfully",
                data,
                pagination: {
                    total,
                    page,
                    limit,
                    totalPages: Math.ceil(total / limit),
                },
            });

        } catch (error) {
            console.error("Fetch Contracts Error:", error);
            return res.status(500).json({
                success: false,
                message: error.message || "Internal server error",
            });
        }
    }

    //getContractById method
    async getContractById(req, res, next) {
        try {
            const { id } = req.query; // ✅ same as getBankDetailById

            if (!id) {
                return res.status(400).json({
                    success: false,
                    message: "Contract ID is required",
                });
            }

            const contract = await Contract.findById(id);

            if (!contract) {
                return res.status(404).json({
                    success: false,
                    message: "Contract not found",
                });
            }

            // ✅ Fetch user info using user_id field (numeric)
            const user = await User.findOne({ id: contract.user_id }, { name: 1 });

            return ResponseService.success(res, "Contract fetched successfully", {
                data: {
                    ...contract.toObject(),
                    userName: user ? user.name : null,
                },
            });

        } catch (error) {
            console.error("Get Contract Error:", error);
            next(error);
        }
    }

    //deleteContract method
    async deleteContract(req, res, next) {
        try {
            const { id } = req.params;

            if (!id) {
                return res.status(400).json({
                    success: false,
                    message: "Contract ID is required",
                });
            }

            // ✅ Find existing contract
            const existingContract = await Contract.findById(id);
            if (!existingContract) {
                return res.status(404).json({
                    success: false,
                    message: "Contract not found",
                });
            }

            // ✅ Delete file if it exists
            if (existingContract.pdf) {
                const filePath = path.join(__dirname, "../uploads/contracts", existingContract.pdf);
                if (fs.existsSync(filePath)) {
                    fs.unlinkSync(filePath);
                }
            }

            // ✅ Delete contract from DB
            await Contract.deleteOne({ _id: id });

            // ✅ Create log entry (store full JSON)
            await ContractLog.create({
                user_id: existingContract.user_id,
                contract_id: existingContract._id,
                action: "delete",
                data: existingContract, // store entire contract JSON
                message: `Contract "${existingContract.contractName}" deleted.`,
                ipAddress: req.ip,
                timestamp: new Date(),
            });

            return res.status(200).json({
                success: true,
                message: "Contract deleted successfully",
            });

        } catch (error) {
            console.error("Delete Contract Error:", error);
            next(error);
        }
    }

    //getContractLogs method
    async getContractLogs(req, res, next) {
        try {
            let { page = 1, limit = 10, search = "" } = req.query;
            page = Number(page);
            limit = Number(limit);

            // ✅ Build search query
            const query = search
                ? {
                    $or: [
                        { action: { $regex: search, $options: "i" } },
                        { message: { $regex: search, $options: "i" } },
                    ],
                }
                : {};

            // ✅ Total count for pagination
            const total = await ContractLog.countDocuments(query);

            // ✅ Fetch paginated logs
            const logs = await ContractLog.find(query)
                .populate("contract_id", "contractName")
                .sort({ createdAt: -1 })
                .skip((page - 1) * limit)
                .limit(limit)
                .lean();

            // ✅ Extract all unique numeric user_ids
            const userIds = [...new Set(logs.map((log) => log.user_id).filter(Boolean))];

            // ✅ Fetch user info manually from User collection (matching id, not _id)
            const users = await User.find(
                { id: { $in: userIds } },
                { id: 1, name: 1, email: 1 }
            ).lean();

            // ✅ Map users by numeric id for quick lookup
            const userMap = {};
            users.forEach((u) => {
                userMap[u.id] = u;
            });

            // ✅ Attach user info to logs
            const logsWithUsers = logs.map((log) => ({
                ...log,
                user: userMap[log.user_id] || null,
            }));

            // ✅ Return paginated response
            return res.status(200).json({
                success: true,
                message: "Contract logs fetched successfully",
                data: logsWithUsers,
                pagination: {
                    total,
                    page,
                    limit,
                    totalPages: Math.ceil(total / limit),
                },
            });
        } catch (error) {
            next(error)
        }
    }

    //getContractLogById method
    async getContractLogById(req, res, next) {
        try {
            const { contract_id } = req.query;

            if (!contract_id) {
                return res.status(400).json({
                    success: false,
                    message: "contract_id is required in query",
                });
            }

            // Fetch logs only for this contract
            const logs = await ContractLog.find({ contract_id })
                .populate("contract_id", "contractName")
                .sort({ createdAt: -1 })
                .lean();

            // Get all unique numeric user IDs
            const userIds = [...new Set(logs.map((log) => log.user_id).filter(Boolean))];

            //Fetch users manually by numeric ID
            const users = await User.find(
                { id: { $in: userIds } },
                { id: 1, name: 1, email: 1 }
            ).lean();

            //Create a user map for quick lookup
            const userMap = {};
            users.forEach((u) => (userMap[u.id] = u));

            //Attach user info to logs
            const logsWithUsers = logs.map((log) => ({
                ...log,
                user: userMap[log.user_id] || null,
            }));

            return res.status(200).json({
                success: true,
                message: "Contract logs fetched successfully",
                data: logsWithUsers,
            });
        } catch (error) {
            console.error("Get Contract Logs Error:", error);
            next(error);
        }
    }

    //sendContractReminder method
    async sendContractReminder(req, res, next) {
        try {
            const { id } = req.params;

            // Find contract by ID
            const contract = await Contract.findById(id);
            if (!contract) {
                return res.status(404).json({ success: false, message: "Contract not found" });
            }

            // Find user manually (since user_id is a Number)
            const user = await User.findOne({ id: contract.user_id }, "email name");
            if (!user) {
                return res.status(400).json({ success: false, message: "Client not found" });
            }

            // Prepare email content
            const { contractName, endDate } = contract;
            const subject = `Reminder: Contract "${contractName}" Expiring Soon`;

            // Use imported template
            const html = contractReminderTemplate(user.name, contractName, endDate);

            // Send email
            // await sendEmail(user.email, subject, html);
            await sendEmail('tripathipawan1187@gmail.com', subject, html);

            res.status(200).json({ success: true, message: "Reminder email sent successfully" });

        } catch (error) {
            console.error("Error sending reminder email:", error);
            res.status(500).json({ success: false, message: "Error sending reminder email" });
        }
    }

    //sendContractWhatsappReminder method
    async sendContractWhatsappReminder(req, res, next) {
        try {
            const { id } = req.params;

            // Find contract
            const contract = await Contract.findById(id);
            console.log("contract", contract);
            return

            if (!contract) {
                return res.status(404).json({ success: false, message: "Contract not found" });
            }

            // Find user
            const user = await User.findOne(
                { id: contract.user_id },
                "phone name"
            );

            if (!user || !user.phone) {
                return res
                    .status(400)
                    .json({ success: false, message: "Client phone number not found" });
            }

            // WhatsApp message content
            const message = `Hello ${user.name}, your contract "${contract.contractName}" is expiring on ${contract.endDate}. Please take necessary action.`;

            // Call Messente API
            // await sendWhatsappMessage({
            //     to: user.phone,
            //     message
            // });

            await sendWhatsappMessage({
                to: 8726099200,
                message
            });

            return res.status(200).json({
                success: true,
                message: "WhatsApp reminder sent successfully"
            });

        } catch (error) {
            console.error("Error sending WhatsApp reminder:", error);
            res.status(500).json({
                success: false,
                message: "Error sending WhatsApp reminder"
            });
        }
    }

    //fetchLabelAndSubLabelContract method
    async fetchLabelAndSubLabelContract(req, res) {
        try {
            const { id } = req.query;

            if (!id) {
                return ResponseService.error(res, "id is required", 400);
            }

            const numericId = Number(id);

            // Fetch all sub-users
            const users = await User.find({ parent_id: numericId });

            const userIds = users.map(u => u.id);

            // Aggregation to fetch contracts + user name
            const contract = await Contract.aggregate([
                {
                    $match: {
                        user_id: { $in: userIds }
                    }
                },
                {
                    $lookup: {
                        from: "users",               // MongoDB collection name
                        localField: "user_id",
                        foreignField: "id",
                        as: "userInfo"
                    }
                },
                {
                    $unwind: {
                        path: "$userInfo",
                        preserveNullAndEmptyArrays: true
                    }
                },
                {
                    $project: {
                        _id: 1,
                        user_id: 1,
                        contractName: 1,
                        pdf: 1,
                        startDate: 1,
                        endDate: 1,
                        labelPercentage: 1,
                        description: 1,
                        status: 1,
                        userName: "$userInfo.name"  // 👈 Add username here
                    }
                }
            ]);

            return ResponseService.success(
                res,
                "Contract details fetched successfully",
                { contract }
            );

        } catch (error) {
            return res.status(500).json({
                success: false,
                message: error.message
            });
        }
    }

}

module.exports = new contractController();