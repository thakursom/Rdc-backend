const fs = require('fs');
const path = require('path');
const XLSX = require("xlsx");

const LogService = require("../services/logService");
const User = require("../models/userModel");
const RevenueUpload = require("../models/RevenueUploadModel");
const AppleRevenue = require("../models/AppleRevenueModel");
const SpotifyRevenue = require("../models/SpotifyRevenueModel");
const GaanaRevenue = require("../models/GaanaRevenueModel");
const JioSaavanRevenue = require("../models/JioSaavanRevenueModel");
const FacebookRevenue = require("../models/FacebookRevenueModel");
const AmazonRevenue = require("../models/AmazonRevenueModel");
const TikTokRevenue = require("../models/TikTokRevenueModel");
const TempReport = require("../models/tempReportModel");
const TblReport2025 = require("../models/tblReport2025Model");
const SoundRecordingRevenue = require("../models/soundRecordingRevenueModel");
const YouTubeArtTrackRevenue = require("../models/youTubeArtTrackRevenueModel");
const YouTubePartnerChannelRevenue = require("../models/youTubePartnerChannelRevenueModel");
const YouTubeRDCChannelRevenue = require("../models/youTubeRDCChannelRevenueModel");
const YouTubeVideoClaimRevenue = require("../models/youTubeVideoClaimRevenueModel");
const YTPremiumRevenue = require("../models/ytPremiumRevenueModel");
const { excelSerialToISODate } = require("../utils/dateUtils");
const Release = require("../models/releaseModel");
const Contract = require("../models/contractModel");
const AudioStreamingReportHistory = require("../models/audioStreamingReportHistoryModel");
const YoutubeReportHistory = require("../models/youtubeReportHistoryModel");



class revenueUploadController {

    constructor() {

        this.processPendingReports = this.processPendingReports.bind(this);
        this.processAudioStreamingReport = this.processAudioStreamingReport.bind(this);
        this.processPendingYoutubeReports = this.processPendingYoutubeReports.bind(this);
        this.processYoutubeReport = this.processYoutubeReport.bind(this);
    }


    //uploadRevenue method
    async uploadRevenue(req, res, next) {
        try {
            const { userId, email } = req.user;
            const { platform, periodFrom, periodTo } = req.body;

            if (!req.file) {
                return res.status(400).json({ error: "No file uploaded" });
            }

            const relativePath = `uploads/revenues/${req.file.filename}`;
            const fileURL = `${process.env.BASE_URL}/${relativePath}`;

            // Read Excel
            const workbook = XLSX.readFile(req.file.path);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const jsonData = XLSX.utils.sheet_to_json(sheet);

            if (jsonData.length === 0) {
                return res.status(400).json({ error: "Excel file is empty" });
            }

            // Process all rows and collect ISRC codes for batch lookup
            const rowsWithIsrc = [];
            const isrcCodes = new Set();

            // First pass: extract ISRC codes and prepare row data
            jsonData.forEach(r => {
                let isrcCode = null;
                let obj = {};

                // FACEBOOK MAPPING
                if (platform === "Facebook") {
                    isrcCode = r.elected_isrc;
                    obj = {
                        retailer: r.service || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r.track_title || null,
                        track_title: r.track_title || null,
                        track_artist: r.track_artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r.event_count_1 || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };

                    // SPOTIFY MAPPING
                } else if (platform === "Spotify") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: "Spotify",
                        label: r["Label Name"] || null,
                        upc_code: r.EAN || null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Album name"] || null,
                        track_title: r["Track name"] || null,
                        track_artist: r["Artist name"] || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r.Quantity || null,
                        sale_type: null,
                        net_total: r.Total || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };

                    // Amazon MAPPING
                } else if (platform === "Amazon") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: "Amazon",
                        label: r["Label Name"] || null,
                        upc_code: r["Digital Album Upc"] || null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Album Name"] || null,
                        track_title: r["Track Name"] || null,
                        track_artist: r["Artist Name"] || null,
                        remixer_name: null,
                        remix: null,
                        territory: r["territory code"] || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r["Total Plays"] || null,
                        sale_type: null,
                        net_total: r[" Total Revenue"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };

                    //JioSaavan MAPPING
                } else if (platform === "JioSaavan") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: "jio_savan",
                        label: r["Label Name"] || null,
                        upc_code: r.UPC || null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Album Name"] || null,
                        track_title: r["Track Name"] || null,
                        track_artist: r["Artist Name"] || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r["Total Streams"] || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                        date: excelSerialToISODate(r.Months) || null,
                    };

                    //AppleItunes MAPPING
                } else if (platform === "AppleItunes") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: "Apple Music",
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Item Title"] || null,
                        track_title: r["Item Title"] || null,
                        track_artist: r["Item Artist"] || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r.Quantity || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };

                    //TikTok MAPPING
                } else if (platform === "TikTok") {
                    isrcCode = r.Isrc;
                    obj = {
                        retailer: r.Platforn_Name,
                        label: r["Label Name"] || null,
                        upc_code: r.Product_Code,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r.Album || null,
                        track_title: r.Song_Title || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r.Views || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                        date: excelSerialToISODate(r.Months) || null,
                    };

                    //Gaana MAPPING
                } else if (platform === "Gaana") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: "Gaana",
                        label: r["Label Name"] || null,
                        upc_code: r["Album UPC"],
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r.Album || null,
                        track_title: r["Track Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r.content_type || null,
                        track_count: r["Total Playouts"] || null,
                        sale_type: null,
                        net_total: r["Total"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "SoundRecording") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: r["Channel Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: r.UPC,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r.Album || null,
                        track_title: r["Asset Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r["Owned Views"] || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "YouTubeArtTrack") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: r["Channel Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: r.UPC,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Asset Title"] || null,
                        track_title: r["Asset Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r["Content Type"] || null,
                        track_count: r["Owned Views"] || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "YouTubePartnerChannel") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: r["Channel Display Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Asset Title"] || null,
                        track_title: r["Asset Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r["Content Type"] || null,
                        track_count: r["Owned Views"] || null,
                        sale_type: null,
                        net_total: r["Total INR"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "YouTubeRDCChannel") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: r["Channel Display Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Asset Title"] || null,
                        track_title: r["Asset Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r["Content Type"] || null,
                        track_count: r["Owned Views"] || null,
                        sale_type: null,
                        net_total: r["Total INR"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "YouTubeVideoClaim") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: r["Channel Display Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Asset Title"] || null,
                        track_title: r["Asset Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r["Content Type"] || null,
                        track_count: r["Owned Views"] || null,
                        sale_type: null,
                        net_total: r["Total INR"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "YTPremiumRevenue") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: r["Channel Display Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Asset Title"] || null,
                        track_title: r["Asset Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r["Content Type"] || null,
                        track_count: r["Owned Views"] || null,
                        sale_type: null,
                        net_total: r["Total INR"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                }

                if (isrcCode) {
                    isrcCodes.add(isrcCode);
                }

                rowsWithIsrc.push({
                    data: obj,
                    isrc: isrcCode
                });
            });

            // Batch lookup for ISRC codes in Release model
            const isrcToLabelMap = {};
            if (isrcCodes.size > 0) {
                const releases = await Release.find({
                    isrc: { $in: Array.from(isrcCodes) },
                    deleted: 0
                }).select('isrc label_id');

                releases.forEach(release => {
                    isrcToLabelMap[release.isrc] = release.label_id;
                });
            }

            const RevenueUploads = await RevenueUpload.create({
                user_id: userId,
                platform,
                periodFrom: periodFrom || null,
                periodTo: periodTo || null,
                fileName: req.file.filename,
                filePath: fileURL,
                fileExt: req.file.mimetype,
            });

            // Prepare final mapped rows with label_id
            const mappedRows = [];

            rowsWithIsrc.forEach(row => {
                let labelId = 0;

                if (row.isrc && isrcToLabelMap[row.isrc]) {
                    labelId = isrcToLabelMap[row.isrc];
                }

                const today = new Date().toISOString().split("T")[0];
                const finalRow = {
                    ...row.data,
                    user_id: labelId,
                    uploading_date: today,
                    uploadId: RevenueUploads._id
                };

                mappedRows.push(finalRow);
            });

            const modelMap = {
                Spotify: SpotifyRevenue,
                AppleItunes: AppleRevenue,
                Gaana: GaanaRevenue,
                JioSaavan: JioSaavanRevenue,
                Facebook: FacebookRevenue,
                Amazon: AmazonRevenue,
                TikTok: TikTokRevenue,
                SoundRecording: SoundRecordingRevenue,
                YouTubeArtTrack: YouTubeArtTrackRevenue,
                YouTubePartnerChannel: YouTubePartnerChannelRevenue,
                YouTubeRDCChannel: YouTubeRDCChannelRevenue,
                YouTubeVideoClaim: YouTubeVideoClaimRevenue,
                YTPremiumRevenue: YTPremiumRevenue
            };

            if (modelMap[platform]) {
                await modelMap[platform].insertMany(mappedRows);
            }

            // Now insert the new mapped rows
            await TempReport.insertMany(mappedRows);

            await LogService.createLog({
                user_id: userId,
                email,
                action: `ADD_REVENUE_FOR_${platform}`,
                description: `${platform} revenue uploaded successfully`,
                newData: mappedRows,
                req
            });

            return res.json({
                success: true,
                message: "File uploaded and processed successfully",
                uploadId: RevenueUploads._id,
                fileURL: fileURL
            });

        } catch (error) {
            next(error);
        }
    }

    //getAllRevenueUploads method
    async getAllRevenueUploads(req, res, next) {
        try {
            let { page = 1, limit = 20, platform } = req.query;

            page = parseInt(page);
            limit = parseInt(limit);

            const query = {};

            // Optional filter by platform
            if (platform) {
                query.platform = platform;
            }

            const skip = (page - 1) * limit;

            const [data, total] = await Promise.all([
                RevenueUpload.find(query)
                    .sort({ createdAt: -1 })
                    .skip(skip)
                    .limit(limit),

                RevenueUpload.countDocuments(query)
            ]);

            return res.json({
                success: true,
                message: "Revenue uploads fetched successfully",
                currentPage: page,
                totalPages: Math.ceil(total / limit),
                totalRecords: total,
                data
            });

        } catch (error) {
            next(error)
        }
    }

    //getRevenueById method
    async getRevenueById(req, res, next) {
        try {
            const { userId, page = 1, limit = 20 } = req.query;

            if (!userId) {
                return res.status(400).json({
                    success: false,
                    message: "User ID is required"
                });
            }

            // const numericUserId = parseInt(userId);
            const pageNum = parseInt(page);
            const limitNum = parseInt(limit);
            const skip = (pageNum - 1) * limitNum;

            const revenues = await TempReport.find({
                uploadId: userId
            })
                .sort({ uploading_date: -1 })
                .skip(skip)
                .limit(limitNum);

            const totalCount = await TempReport.countDocuments({
                uploadId: userId
            });

            const totalPages = Math.ceil(totalCount / limitNum);

            return res.status(200).json({
                success: true,
                message: "Revenue data retrieved successfully",
                data: revenues,
                pagination: {
                    currentPage: pageNum,
                    totalPages: totalPages,
                    totalCount: totalCount,
                    hasNext: pageNum < totalPages,
                    hasPrev: pageNum > 1
                }
            });

        } catch (error) {
            console.error("Error fetching revenue by user ID:", error);
            return res.status(500).json({
                success: false,
                message: "Internal server error"
            });
        }
    }

    //uploadTblRevenue method
    async uploadTblRevenue(req, res, next) {
        try {
            const { userId, email } = req.user;
            const { uploadId } = req.query;

            if (!uploadId) {
                return res.status(400).json({
                    success: false,
                    message: "uploadId required"
                });
            }

            //  Accept the upload
            const revenueUpload = await RevenueUpload.findByIdAndUpdate(
                uploadId,
                { isAccepted: true },
                { new: true }
            );

            if (!revenueUpload) {
                return res.status(404).json({
                    success: false,
                    message: "Revenue upload not found"
                });
            }

            //  Fetch TempReport data
            const tempData = await TempReport.find({ uploadId }).lean();

            if (!tempData.length) {
                return res.status(404).json({
                    success: false,
                    message: "No data found for this uploadId"
                });
            }

            const cleanedData = tempData.map(({ _id, ...rest }) => rest);

            //  Collect unique user_ids
            const userIds = [...new Set(
                cleanedData
                    .map(r => r.user_id)
                    .filter(id => id !== null && id !== 0)
            )];

            //  Fetch active contracts
            const contracts = await Contract.find({
                user_id: { $in: userIds },
                status: "active"
            }).lean();

            //  Helper for safe date conversion
            const toDate = (d) => {
                if (!d) return null;
                const parsed = new Date(d);
                return isNaN(parsed) ? null : parsed;
            };

            //  Apply percentage to each row
            const finalData = cleanedData.map(row => {
                const rowDate = toDate(row.date);
                let percentage = 0;

                if (rowDate) {
                    const matchedContract = contracts.find(contract =>
                        contract.user_id === row.user_id &&
                        rowDate >= new Date(contract.startDate) &&
                        rowDate <= new Date(contract.endDate)
                    );

                    if (matchedContract) {
                        percentage = matchedContract.labelPercentage || 0;
                    }
                }

                return {
                    ...row,
                    percentage
                };
            });

            //  Insert into TblReport2025
            await TblReport2025.insertMany(finalData);

            //  Clear TempReport
            await TempReport.deleteMany({ uploadId });

            // Logging
            await LogService.createLog({
                user_id: userId,
                email,
                action: `REVENUE_ADDED_IN_TBLREPORT_FOR_${tempData[0].retailer}`,
                description: `${tempData[0].retailer} revenue uploaded successfully in tbl_report`,
                newData: finalData,
                req
            });

            return res.status(200).json({
                success: true,
                message: "Data moved from TempReport to TblReport_2025 successfully",
                insertedCount: finalData.length
            });

        } catch (error) {
            console.error("uploadTblRevenue error:", error);
            return res.status(500).json({
                success: false,
                message: "Something went wrong",
                error: error.message
            });
        }
    }

    // getAudioStreamingRevenueReport method
    async getAudioStreamingRevenueReport(req, res, next) {
        try {
            const {
                labelId,
                platform,
                year,
                month,
                fromDate,
                toDate,
                releases,
                artist,
                track,
                partner,
                contentType,
                format,
                territory,
                quarters,
                page = 1,
                limit = 10,
            } = req.query;

            const { role, userId } = req.user;

            const userFilter = {};

            if (userId && role) {
                if (role !== "Super Admin" && role !== "Manager") {
                    // For non-Super Admin/Manager users, get child users
                    const users = await User.find({ parent_id: userId }, { id: 1 });
                    const childIds = users.map(u => u.id);
                    childIds.push(userId);
                    userFilter.user_id = { $in: childIds };
                }
            }

            const defaultRetailers = [
                "Apple Music",
                "Spotify",
                "Gaana",
                "Jio Saavn",
                "Facebook",
                "Amazon",
                "TikTok"
            ];

            // -------- BUILD FILTER --------
            const filter = { ...userFilter };

            if (labelId) {
                filter.user_id = Number(labelId);
            }

            if (platform && platform !== "") {
                const platforms = platform.split(",").map(p => p.trim());
                filter.retailer = { $in: platforms };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();

            // Year only
            if (year && !month && !fromDate && !toDate) {
                filter.date = {
                    $gte: `${selectedYear}-01-01`,
                    $lte: `${selectedYear}-12-31`
                };
            }

            // Month + Year
            if (month && month !== '') {
                const startDate = new Date(selectedYear, parseInt(month) - 1, 1);
                const endDate = new Date(selectedYear, parseInt(month), 0);
                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            // Custom date range
            if (fromDate && toDate) {
                const [fromYear, fromMonth] = fromDate.split("-").map(Number);
                const [toYear, toMonth] = toDate.split("-").map(Number);

                const startDate = new Date(fromYear, fromMonth - 1, 1);
                const endDate = new Date(toYear, toMonth, 0);

                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            // Checkbox filters
            if (artist === "true") filter.track_artist = { $nin: ["", null, undefined] };
            if (territory === "true") filter.territory = { $nin: ["", null, undefined] };
            if (releases === "true") filter.release = { $nin: ["", null, undefined] };

            // Convert net_total safely
            const addSafeRevenue = {
                $addFields: {
                    safeRevenue: {
                        $convert: {
                            input: "$net_total",
                            to: "double",
                            onError: 0,
                            onNull: 0
                        }
                    }
                }
            };

            const detailedPipeline = [
                { $match: filter },
                addSafeRevenue,
                {
                    $project: {
                        date: 1,
                        retailer: 1,
                        release: 1,
                        track_artist: 1,
                        safeRevenue: 1,
                        user_id: 1,
                        territory: 1,
                        track_count: 1
                    }
                }
            ];

            const detailedData = await TblReport2025.aggregate(detailedPipeline);

            // Get all unique user_ids from the data
            const allUserIds = [...new Set(detailedData.map(item => item.user_id).filter(id => id !== null))];

            // Get contracts for these users
            let allContracts = [];
            if (allUserIds.length > 0) {
                allContracts = await Contract.find({
                    user_id: { $in: allUserIds },
                    status: "active"
                }).lean();
            }

            const processedData = detailedData.map(item => {
                let deductedRevenue = item.safeRevenue;
                let appliedPercentage = 0;
                let contractApplied = false;

                // Find matching contract for this user and date
                if (item.user_id && allContracts.length > 0) {
                    const userContracts = allContracts.filter(contract =>
                        contract.user_id === item.user_id
                    );

                    for (const contract of userContracts) {
                        if (item.date >= contract.startDate && item.date <= contract.endDate) {
                            const labelPercentage = contract.labelPercentage || 0;
                            const deductionMultiplier = (100 - labelPercentage) / 100;
                            deductedRevenue = item.safeRevenue * deductionMultiplier;
                            appliedPercentage = labelPercentage;
                            contractApplied = true;
                            break;
                        }
                    }
                }

                return {
                    ...item,
                    deductedRevenue: deductedRevenue,
                    originalRevenue: item.safeRevenue,
                    contractApplied: contractApplied,
                    deductionPercentage: appliedPercentage
                };
            });

            // 1. Group by artist for main table
            const artistGroups = {};
            processedData.forEach(item => {
                const artist = item.track_artist || "Unknown Artist";
                if (!artistGroups[artist]) {
                    artistGroups[artist] = {
                        totalRevenue: 0,
                        user_id: item.user_id,
                        firstDate: item.date,
                        firstRetailer: item.retailer,
                        firstRelease: item.release,
                        artistName: artist
                    };
                }

                artistGroups[artist].totalRevenue += item.deductedRevenue;
            });

            // Convert to array and sort
            const allArtistsData = Object.values(artistGroups)
                .map(item => ({
                    date: item.firstDate,
                    platform: item.firstRetailer,
                    artist: item.artistName,
                    release: item.firstRelease,
                    revenue: Number(item.totalRevenue.toFixed(2)),
                    user_id: item.user_id
                }))
                .sort((a, b) => b.revenue - a.revenue);

            // Apply pagination
            const startIndex = (parseInt(page) - 1) * parseInt(limit);
            const endIndex = startIndex + parseInt(limit);
            const paginatedResult = allArtistsData.slice(startIndex, endIndex);

            // 2. Count total artists
            const totalRecords = Object.keys(artistGroups).length;
            const totalPages = Math.ceil(totalRecords / parseInt(limit));

            // 3. Summary (total streams and revenue)
            const summary = processedData.reduce((acc, item) => {
                acc.totalStreams += parseInt(item.track_count) || 0;
                acc.totalRevenue += item.deductedRevenue;
                return acc;
            }, { totalStreams: 0, totalRevenue: 0 });

            // 4. Revenue by Month
            const monthGroups = {};
            processedData.forEach(item => {
                const date = new Date(item.date);
                const monthKey = `${date.toLocaleString('default', { month: 'short' })} ${date.getFullYear()}`;

                if (!monthGroups[monthKey]) {
                    monthGroups[monthKey] = 0;
                }

                monthGroups[monthKey] += item.deductedRevenue;
            });

            const byMonthResult = Object.entries(monthGroups)
                .map(([monthLabel, revenue]) => ({ monthLabel, revenue: Number(revenue.toFixed(2)) }))
                .sort((a, b) => {
                    const dateA = new Date(a.monthLabel);
                    const dateB = new Date(b.monthLabel);
                    return dateA - dateB;
                });

            // 5. Revenue by Channel
            const channelGroups = {};
            processedData.forEach(item => {
                const channel = item.retailer || "Unknown";
                if (!channelGroups[channel]) {
                    channelGroups[channel] = 0;
                }

                channelGroups[channel] += item.deductedRevenue;
            });

            const byChannelResult = Object.entries(channelGroups)
                .map(([platform, revenue]) => ({ platform, revenue: Number(revenue.toFixed(2)) }))
                .sort((a, b) => b.revenue - a.revenue);

            // 6. Revenue by Country
            const countryGroups = {};
            processedData.forEach(item => {
                const country = item.territory || "Unknown";
                if (!countryGroups[country]) {
                    countryGroups[country] = 0;
                }

                countryGroups[country] += item.deductedRevenue;
            });

            const byCountryResult = Object.entries(countryGroups)
                .map(([country, revenue]) => ({ country, revenue: Number(revenue.toFixed(2)) }))
                .sort((a, b) => b.revenue - a.revenue)
                .slice(0, 10);

            // Calculate stats about deductions
            const entriesWithDeduction = processedData.filter(item => item.contractApplied).length;
            const totalEntries = processedData.length;
            let avgDeductionPercentage = 0;

            if (entriesWithDeduction > 0) {
                const totalDeduction = processedData
                    .filter(item => item.contractApplied)
                    .reduce((sum, item) => sum + item.deductionPercentage, 0);
                avgDeductionPercentage = totalDeduction / entriesWithDeduction;
            }

            // Format revenueByChannel object with default retailers
            // const revenueByChannel = {};
            // defaultRetailers.forEach(platform => {
            //     const found = byChannelResult.find(item => item.platform === platform);
            //     revenueByChannel[platform] = found ? found.revenue : 0;
            // });

            // Format response
            res.json({
                success: true,
                data: {
                    summary: {
                        totalStreams: summary.totalStreams,
                        totalRevenue: Number(summary.totalRevenue.toFixed(2)),
                        deductionApplied: entriesWithDeduction > 0,
                        deductionPercentage: avgDeductionPercentage,
                        entriesWithDeduction: entriesWithDeduction,
                        totalEntries: totalEntries
                    },
                    reports: paginatedResult,
                    pagination: {
                        totalRecords,
                        totalPages,
                        currentPage: parseInt(page),
                        limit: parseInt(limit)
                    },
                    revenueByMonth: Object.fromEntries(
                        byMonthResult.map(item => [item.monthLabel, item.revenue])
                    ),
                    revenueByChannel: Object.fromEntries(
                        byChannelResult.map(item => [item.platform || "Unknown", item.revenue])
                    ),
                    revenueByCountry: Object.fromEntries(
                        byCountryResult.map(item => [item.country || "Unknown", item.revenue])
                    )
                }
            });

        } catch (error) {
            console.error("Error in getRevenueReport:", error);
            next(error);
        }
    }

    // getYoutubeRevenueReport method
    async getYoutubeRevenueReport(req, res, next) {
        try {
            const {
                labelId,
                platform,
                year,
                month,
                fromDate,
                toDate,
                releases,
                artist,
                track,
                partner,
                contentType,
                format,
                territory,
                quarters,
                page = 1,
                limit = 10,
            } = req.query;

            const { role, userId } = req.user;

            const userFilter = {};

            if (userId && role) {
                if (role !== "Super Admin" && role !== "Manager") {
                    // For non-Super Admin/Manager users, get child users
                    const users = await User.find({ parent_id: userId }, { id: 1 });
                    const childIds = users.map(u => u.id);
                    childIds.push(userId);
                    userFilter.user_id = { $in: childIds };
                }
            }

            const defaultRetailers = [
                "Sound Recording (Audio Claim)",
                "Art Track (YouTube Music)",
                "YouTubePartnerChannel",
                "YouTubeRDCChannel",
                "YouTubeVideoClaim",
                "YTPremiumRevenue",
            ];

            // -------- BUILD FILTER --------
            const filter = { ...userFilter };

            if (labelId) {
                filter.user_id = Number(labelId);
            }

            if (platform && platform !== "") {
                const platforms = platform.split(",").map(p => p.trim());
                filter.retailer = { $in: platforms };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();

            // Year only
            if (year && !month && !fromDate && !toDate) {
                filter.date = {
                    $gte: `${selectedYear}-01-01`,
                    $lte: `${selectedYear}-12-31`
                };
            }

            // Month + Year
            if (month && month !== '') {
                const startDate = new Date(selectedYear, parseInt(month) - 1, 1);
                const endDate = new Date(selectedYear, parseInt(month), 0);
                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            // Custom date range
            if (fromDate && toDate) {
                const [fromYear, fromMonth] = fromDate.split("-").map(Number);
                const [toYear, toMonth] = toDate.split("-").map(Number);

                const startDate = new Date(fromYear, fromMonth - 1, 1);
                const endDate = new Date(toYear, toMonth, 0);

                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            // Checkbox filters
            if (artist === "true") filter.track_artist = { $nin: ["", null, undefined] };
            if (territory === "true") filter.territory = { $nin: ["", null, undefined] };
            if (releases === "true") filter.release = { $nin: ["", null, undefined] };

            // Convert net_total safely
            const addSafeRevenue = {
                $addFields: {
                    safeRevenue: {
                        $convert: {
                            input: "$net_total",
                            to: "double",
                            onError: 0,
                            onNull: 0
                        }
                    }
                }
            };

            // ========== GET ALL REVENUE DATA FIRST ==========
            const detailedPipeline = [
                { $match: filter },
                addSafeRevenue,
                {
                    $project: {
                        date: 1,
                        retailer: 1,
                        release: 1,
                        track_artist: 1,
                        safeRevenue: 1,
                        user_id: 1,
                        territory: 1,
                        track_count: 1
                    }
                }
            ];

            const detailedData = await TblReport2025.aggregate(detailedPipeline);

            // ========== GET ALL CONTRACTS ==========
            // Get all unique user_ids from the data
            const allUserIds = [...new Set(detailedData.map(item => item.user_id).filter(id => id !== null))];

            // Get contracts for these users
            let allContracts = [];
            if (allUserIds.length > 0) {
                allContracts = await Contract.find({
                    user_id: { $in: allUserIds },
                    status: "active"
                }).lean();
            }

            // ========== APPLY CONTRACT DEDUCTION TO EACH REVENUE ENTRY ==========
            const processedData = detailedData.map(item => {
                let deductedRevenue = item.safeRevenue;
                let appliedPercentage = 0;
                let contractApplied = false;

                // Find matching contract for this user and date
                if (item.user_id && allContracts.length > 0) {
                    const userContracts = allContracts.filter(contract =>
                        contract.user_id === item.user_id
                    );

                    // Check each contract to see if the revenue date falls within contract dates
                    for (const contract of userContracts) {
                        // Compare dates as strings (YYYY-MM-DD format)
                        if (item.date >= contract.startDate && item.date <= contract.endDate) {
                            // Apply deduction for this contract
                            const labelPercentage = contract.labelPercentage || 0;
                            const deductionMultiplier = (100 - labelPercentage) / 100;
                            deductedRevenue = item.safeRevenue * deductionMultiplier;
                            appliedPercentage = labelPercentage;
                            contractApplied = true;
                            break; // Use first matching contract
                        }
                    }
                }

                return {
                    ...item,
                    deductedRevenue: deductedRevenue,
                    originalRevenue: item.safeRevenue,
                    contractApplied: contractApplied,
                    deductionPercentage: appliedPercentage
                };
            });

            // ========== CALCULATE AGGREGATIONS ==========

            // 1. Group by artist for main table
            const artistGroups = {};
            processedData.forEach(item => {
                const artist = item.track_artist || "Unknown Artist";
                if (!artistGroups[artist]) {
                    artistGroups[artist] = {
                        totalRevenue: 0,
                        user_id: item.user_id,
                        firstDate: item.date,
                        firstRetailer: item.retailer,
                        firstRelease: item.release,
                        artistName: artist
                    };
                }

                artistGroups[artist].totalRevenue += item.deductedRevenue;
            });

            // Convert to array and sort
            const allArtistsData = Object.values(artistGroups)
                .map(item => ({
                    date: item.firstDate,
                    platform: item.firstRetailer,
                    artist: item.artistName,
                    release: item.firstRelease,
                    revenue: Number(item.totalRevenue.toFixed(2)),
                    user_id: item.user_id
                }))
                .sort((a, b) => b.revenue - a.revenue);

            // Apply pagination
            const startIndex = (parseInt(page) - 1) * parseInt(limit);
            const endIndex = startIndex + parseInt(limit);
            const paginatedResult = allArtistsData.slice(startIndex, endIndex);

            // 2. Count total artists
            const totalRecords = Object.keys(artistGroups).length;
            const totalPages = Math.ceil(totalRecords / parseInt(limit));

            // 3. Summary (total streams and revenue)
            const summary = processedData.reduce((acc, item) => {
                acc.totalStreams += parseInt(item.track_count) || 0;
                acc.totalRevenue += item.deductedRevenue;
                return acc;
            }, { totalStreams: 0, totalRevenue: 0 });

            // 4. Revenue by Month
            const monthGroups = {};
            processedData.forEach(item => {
                const date = new Date(item.date);
                const monthKey = `${date.toLocaleString('default', { month: 'short' })} ${date.getFullYear()}`;

                if (!monthGroups[monthKey]) {
                    monthGroups[monthKey] = 0;
                }

                monthGroups[monthKey] += item.deductedRevenue;
            });

            const byMonthResult = Object.entries(monthGroups)
                .map(([monthLabel, revenue]) => ({ monthLabel, revenue: Number(revenue.toFixed(2)) }))
                .sort((a, b) => {
                    const dateA = new Date(a.monthLabel);
                    const dateB = new Date(b.monthLabel);
                    return dateA - dateB;
                });

            // 5. Revenue by Channel
            const channelGroups = {};
            processedData.forEach(item => {
                const channel = item.retailer || "Unknown";
                if (!channelGroups[channel]) {
                    channelGroups[channel] = 0;
                }

                channelGroups[channel] += item.deductedRevenue;
            });

            const byChannelResult = Object.entries(channelGroups)
                .map(([platform, revenue]) => ({ platform, revenue: Number(revenue.toFixed(2)) }))
                .sort((a, b) => b.revenue - a.revenue);

            // 6. Revenue by Country
            const countryGroups = {};
            processedData.forEach(item => {
                const country = item.territory || "Unknown";
                if (!countryGroups[country]) {
                    countryGroups[country] = 0;
                }

                countryGroups[country] += item.deductedRevenue;
            });

            const byCountryResult = Object.entries(countryGroups)
                .map(([country, revenue]) => ({ country, revenue: Number(revenue.toFixed(2)) }))
                .sort((a, b) => b.revenue - a.revenue)
                .slice(0, 10);

            // Calculate stats about deductions
            const entriesWithDeduction = processedData.filter(item => item.contractApplied).length;
            const totalEntries = processedData.length;
            let avgDeductionPercentage = 0;

            if (entriesWithDeduction > 0) {
                const totalDeduction = processedData
                    .filter(item => item.contractApplied)
                    .reduce((sum, item) => sum + item.deductionPercentage, 0);
                avgDeductionPercentage = totalDeduction / entriesWithDeduction;
            }

            // Format revenueByChannel object with default retailers
            // const revenueByChannel = {};
            // defaultRetailers.forEach(platform => {
            //     const found = byChannelResult.find(item => item.platform === platform);
            //     revenueByChannel[platform] = found ? found.revenue : 0;
            // });

            // Format response
            res.json({
                success: true,
                data: {
                    summary: {
                        totalStreams: summary.totalStreams,
                        totalRevenue: Number(summary.totalRevenue.toFixed(2)),
                        deductionApplied: entriesWithDeduction > 0,
                        deductionPercentage: avgDeductionPercentage,
                        entriesWithDeduction: entriesWithDeduction,
                        totalEntries: totalEntries
                    },
                    reports: paginatedResult,
                    pagination: {
                        totalRecords,
                        totalPages,
                        currentPage: parseInt(page),
                        limit: parseInt(limit)
                    },
                    revenueByMonth: Object.fromEntries(
                        byMonthResult.map(item => [item.monthLabel, item.revenue])
                    ),
                    revenueByChannel: Object.fromEntries(
                        byChannelResult.map(item => [item.platform || "Unknown", item.revenue])
                    ),
                    revenueByCountry: Object.fromEntries(
                        byCountryResult.map(item => [item.country || "Unknown", item.revenue])
                    )
                }
            });

        } catch (error) {
            console.error("Error in getRevenueReport:", error);
            next(error);
        }
    }

    // downloadExcelReport method
    async triggerAudioStreamingExcelReport(req, res, next) {
        try {
            const { userId } = req.user;

            const existingReport = await AudioStreamingReportHistory.findOne({
                'filters': req.query,
                status: 'pending'
            });

            if (existingReport) {
                return res.status(200).json({
                    success: true,
                    message: "Report is already being prepared",
                    reportId: existingReport._id
                });
            }

            // Create report with "pending" status
            const newReport = new AudioStreamingReportHistory({
                user_id: userId,
                filters: req.query,
                status: 'pending',
                generatedAt: new Date(),
                filename: 'Generating...'
            });

            await newReport.save();

            return res.status(200).json({
                success: true,
                message: "Report generation started",
                reportId: newReport._id
            });

        } catch (error) {
            console.error("Error triggering report:", error);
            return res.status(500).json({
                success: false,
                message: "Error starting report generation",
                error: error.message
            });
        }
    }

    // Separate function to process the report
    async processAudioStreamingReport(reportId, filters) {
        try {
            console.log(`Processing report ${reportId} with filters:`, filters);

            const {
                userId,
                role,
                labelId,
                platform,
                year,
                month,
                fromDate,
                toDate,
                releases,
                artist,
                track,
                partner,
                contentType,
                format,
                territory,
                quarters
            } = filters;

            const userFilter = {};

            if (userId && role) {
                if (role !== "Super Admin" && role !== "Manager") {
                    // For non-Super Admin/Manager users, get child users
                    const users = await User.find({ parent_id: userId }, { id: 1 });
                    const childIds = users.map(u => u.id);
                    childIds.push(userId);
                    userFilter.user_id = { $in: childIds };
                }
            }

            const defaultRetailers = [
                "Apple Music",
                "Spotify",
                "Gaana",
                "Jio Saavn",
                "Facebook",
                "Amazon",
                "TikTok"
            ];

            const filter = { ...userFilter };

            if (labelId) {
                filter.user_id = Number(labelId);
            }

            if (platform && platform !== "") {
                const platforms = platform.split(",").map(p => p.trim());
                filter.retailer = { $in: platforms };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();

            if (year && !month && !fromDate && !toDate) {
                filter.date = {
                    $gte: `${selectedYear}-01-01`,
                    $lte: `${selectedYear}-12-31`
                };
            }

            if (month && month !== '') {
                const startDate = new Date(selectedYear, parseInt(month) - 1, 1);
                const endDate = new Date(selectedYear, parseInt(month), 0);
                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            if (fromDate && toDate) {
                const [fromYear, fromMonth] = fromDate.split("-").map(Number);
                const [toYear, toMonth] = toDate.split("-").map(Number);

                const startDate = new Date(fromYear, fromMonth - 1, 1);
                const endDate = new Date(toYear, toMonth, 0);

                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            if (artist === "true") filter.track_artist = { $nin: ["", null, undefined] };
            if (territory === "true") filter.territory = { $nin: ["", null, undefined] };
            if (releases === "true") filter.release = { $nin: ["", null, undefined] };

            const data = await TblReport2025.find(filter)
                .select('-__v')
                .sort({ date: -1 })
                .lean();

            console.log(`Found ${data.length} records for report ${reportId}`);

            if (data.length === 0) {
                await AudioStreamingReportHistory.findByIdAndUpdate(reportId, {
                    status: 'failed',
                    error: 'No data found'
                });
                return;
            }

            const excelData = [];
            const rows = data.map(d => ({ ...d }));
            const excludeFields = ["_id", "date", "createdAt", "updatedAt"];

            const dataKeys = Object.keys(rows[0]).filter(
                key => !excludeFields.includes(key)
            );

            const headers = ["S.No", ...dataKeys];
            excelData.push(headers);

            rows.forEach((row, index) => {
                excelData.push([
                    index + 1,
                    ...dataKeys.map(key => row[key])
                ]);
            });

            const workbook = XLSX.utils.book_new();
            const worksheet = XLSX.utils.aoa_to_sheet(excelData);
            XLSX.utils.book_append_sheet(workbook, worksheet, "Revenue Report");

            const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
            const filename = `Revenue_Report_${timestamp}_${reportId}.xlsx`;

            const relativeFolder = 'reports';
            const absoluteFolder = path.join(__dirname, '../uploads', relativeFolder);

            if (!fs.existsSync(absoluteFolder)) {
                fs.mkdirSync(absoluteFolder, { recursive: true });
            }

            const absoluteFilePath = path.join(absoluteFolder, filename);
            const relativePath = `uploads/reports/${filename}`;
            const fileURL = `${process.env.BASE_URL}/${relativePath}`;

            const excelBuffer = XLSX.write(workbook, {
                type: 'buffer',
                bookType: 'xlsx',
                bookSST: false
            });

            // Save file to public folder
            fs.writeFileSync(absoluteFilePath, excelBuffer);
            console.log(`Excel file saved for report ${reportId} at: ${absoluteFilePath}`);

            // Update DB with relative path and public URL
            await AudioStreamingReportHistory.findByIdAndUpdate(reportId, {
                status: 'ready',
                filename: filename,
                filePath: relativePath,
                fileURL: fileURL
            });

            console.log(`Report ${reportId} processed successfully`);

        } catch (error) {
            console.error(`Error processing report ${reportId}:`, error);

            await AudioStreamingReportHistory.findByIdAndUpdate(reportId, {
                status: 'failed',
                error: error.message
            });
        }
    }

    // Trigger YouTube report generation
    async triggerYoutubeExcelReport(req, res, next) {
        try {
            const { userId } = req.user;

            const existingReport = await YoutubeReportHistory.findOne({
                'filters': req.query,
                status: 'pending'
            });

            if (existingReport) {
                return res.status(200).json({
                    success: true,
                    message: "YouTube report is already being prepared",
                    reportId: existingReport._id
                });
            }

            // Create report with "pending" status
            const newReport = new YoutubeReportHistory({
                user_id: userId,
                filters: req.query,
                status: 'pending',
                generatedAt: new Date(),
                filename: 'Generating...'
            });

            await newReport.save();

            return res.status(200).json({
                success: true,
                message: "YouTube report generation started",
                reportId: newReport._id
            });

        } catch (error) {
            console.error("Error triggering YouTube report:", error);
            return res.status(500).json({
                success: false,
                message: "Error starting YouTube report generation",
                error: error.message
            });
        }
    }

    // Process YouTube report
    async processYoutubeReport(reportId, filters) {
        try {
            console.log(`Processing YouTube report ${reportId} with filters:`, filters);

            const {
                userId,
                role,
                labelId,
                platform,
                year,
                month,
                fromDate,
                toDate,
                releases,
                artist,
                track,
                partner,
                contentType,
                format,
                territory,
                quarters
            } = filters;

            const userFilter = {};

            if (userId && role) {
                if (role !== "Super Admin" && role !== "Manager") {
                    // For non-Super Admin/Manager users, get child users
                    const users = await User.find({ parent_id: userId }, { id: 1 });
                    const childIds = users.map(u => u.id);
                    childIds.push(userId);
                    userFilter.user_id = { $in: childIds };
                }
            }

            const defaultRetailers = [
                "Sound Recording (Audio Claim)",
                "Art Track (YouTube Music)",
                "YouTubePartnerChannel",
                "YouTubeRDCChannel",
                "YouTubeVideoClaim",
                "YTPremiumRevenue",
            ];

            const filter = { ...userFilter };

            if (labelId) {
                filter.user_id = Number(labelId);
            }

            if (platform && platform !== "") {
                const platforms = platform.split(",").map(p => p.trim());
                filter.retailer = { $in: platforms };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();

            if (year && !month && !fromDate && !toDate) {
                filter.date = {
                    $gte: `${selectedYear}-01-01`,
                    $lte: `${selectedYear}-12-31`
                };
            }

            if (month && month !== '') {
                const startDate = new Date(selectedYear, parseInt(month) - 1, 1);
                const endDate = new Date(selectedYear, parseInt(month), 0);
                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            if (fromDate && toDate) {
                const [fromYear, fromMonth] = fromDate.split("-").map(Number);
                const [toYear, toMonth] = toDate.split("-").map(Number);

                const startDate = new Date(fromYear, fromMonth - 1, 1);
                const endDate = new Date(toYear, toMonth, 0);

                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            if (artist === "true") filter.track_artist = { $nin: ["", null, undefined] };
            if (territory === "true") filter.territory = { $nin: ["", null, undefined] };
            if (releases === "true") filter.release = { $nin: ["", null, undefined] };

            const data = await TblReport2025.find(filter)
                .select('-__v')
                .sort({ date: -1 })
                .lean();

            console.log(`Found ${data.length} records for YouTube report ${reportId}`);

            if (data.length === 0) {
                await YoutubeReportHistory.findByIdAndUpdate(reportId, {
                    status: 'failed',
                    error: 'No data found'
                });
                return;
            }

            const excelData = [];
            const rows = data.map(d => ({ ...d }));
            const excludeFields = ["_id", "date", "createdAt", "updatedAt"];

            const dataKeys = Object.keys(rows[0]).filter(
                key => !excludeFields.includes(key)
            );

            const headers = ["S.No", ...dataKeys];
            excelData.push(headers);

            rows.forEach((row, index) => {
                excelData.push([
                    index + 1,
                    ...dataKeys.map(key => row[key])
                ]);
            });

            const workbook = XLSX.utils.book_new();
            const worksheet = XLSX.utils.aoa_to_sheet(excelData);
            XLSX.utils.book_append_sheet(workbook, worksheet, "YouTube Revenue Report");

            const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
            const filename = `YouTube_Revenue_Report_${timestamp}_${reportId}.xlsx`;

            const relativeFolder = 'reports';
            const absoluteFolder = path.join(__dirname, '../uploads', relativeFolder);

            if (!fs.existsSync(absoluteFolder)) {
                fs.mkdirSync(absoluteFolder, { recursive: true });
            }

            const absoluteFilePath = path.join(absoluteFolder, filename);
            const relativePath = `uploads/reports/${filename}`;
            const fileURL = `${process.env.BASE_URL}/${relativePath}`;

            const excelBuffer = XLSX.write(workbook, {
                type: 'buffer',
                bookType: 'xlsx',
                bookSST: false
            });

            // Save file to public folder
            fs.writeFileSync(absoluteFilePath, excelBuffer);
            console.log(`YouTube Excel file saved for report ${reportId} at: ${absoluteFilePath}`);

            // Update DB with relative path and public URL
            await YoutubeReportHistory.findByIdAndUpdate(reportId, {
                status: 'ready',
                filename: filename,
                filePath: relativePath,
                fileURL: fileURL
            });

            console.log(`YouTube report ${reportId} processed successfully`);

        } catch (error) {
            console.error(`Error processing YouTube report ${reportId}:`, error);

            await YoutubeReportHistory.findByIdAndUpdate(reportId, {
                status: 'failed',
                error: error.message
            });
        }
    }

    // Process pending audio streaming reports
    async processPendingReports() {
        try {
            const pendingReports = await AudioStreamingReportHistory.find({
                status: 'pending'
            });

            console.log(`Found ${pendingReports.length} pending audio reports to process`);

            for (const report of pendingReports) {
                const reportAge = Date.now() - new Date(report.generatedAt).getTime();
                const THIRTY_MINUTES = 30 * 60 * 1000;

                if (reportAge > THIRTY_MINUTES) {
                    await AudioStreamingReportHistory.findByIdAndUpdate(report._id, {
                        status: 'failed',
                        error: 'Processing timeout'
                    });
                    continue;
                }

                await this.processAudioStreamingReport(report._id, report.filters);
            }
        } catch (error) {
            console.error("Error in processPendingReports cron job:", error);
        }
    }

    // Process pending YouTube reports
    async processPendingYoutubeReports() {
        try {
            const pendingReports = await YoutubeReportHistory.find({
                status: 'pending'
            });

            console.log(`Found ${pendingReports.length} pending YouTube reports to process`);

            for (const report of pendingReports) {
                const reportAge = Date.now() - new Date(report.generatedAt).getTime();
                const THIRTY_MINUTES = 30 * 60 * 1000;

                if (reportAge > THIRTY_MINUTES) {
                    await YoutubeReportHistory.findByIdAndUpdate(report._id, {
                        status: 'failed',
                        error: 'Processing timeout'
                    });
                    continue;
                }

                await this.processYoutubeReport(report._id, report.filters);
            }
        } catch (error) {
            console.error("Error in processPendingYoutubeReports cron job:", error);
        }
    }

    // Combined cron job to process all pending reports
    async processAllPendingReports() {
        try {
            console.log('Processing all pending reports...');

            // Process audio streaming reports
            await this.processPendingReports();

            // Process YouTube reports
            await this.processPendingYoutubeReports();

            console.log('All pending reports processed');
        } catch (error) {
            console.error("Error in processAllPendingReports:", error);
        }
    }

    // deleteRevenueByUserId method
    async deleteRevenueByUserId(req, res, next) {
        try {
            const { userId } = req.query;

            if (!userId) {
                return res.status(400).json({
                    success: false,
                    message: "User ID is required"
                });
            }

            // Delete upload entry
            const revenueUploadResult = await RevenueUpload.findByIdAndDelete(userId);

            if (!revenueUploadResult) {
                return res.status(404).json({
                    success: false,
                    message: "RevenueUpload record not found"
                });
            }

            // Delete revenue reports
            const tempReportResult = await TempReport.deleteMany({
                uploadId: userId
            });

            return res.status(200).json({
                success: true,
                message: "Revenue data deleted successfully",
                tempReportDeletedCount: tempReportResult.deletedCount,
                revenueUploadDeleted: true
            });

        } catch (error) {
            console.error("Error deleting revenue by user ID:", error);
            return res.status(500).json({
                success: false,
                message: "Internal server error"
            });
        }
    }

    //getReportHistory method
    async getReportHistory(req, res, next) {
        try {
            const { role, userId } = req.user;

            const query = {};

            if (role !== "Super Admin" && role !== "Manager") {
                query.user_id = userId;
            }
            const reports = await AudioStreamingReportHistory.find(query)
                .select('filename filePath fileURL status generatedAt user_id')
                .sort({ generatedAt: -1 })
                .lean();

            return res.status(200).json({
                success: true,
                count: reports.length,
                data: reports
            });

        } catch (error) {
            console.error("Error fetching report history:", error);

            return res.status(500).json({
                success: false,
                message: "Failed to fetch report history",
                error: error.message
            });
        }
    }

    //deleteReportHistory method
    async deleteReportHistory(req, res, next) {
        try {
            const { id } = req.query;

            if (!id) {
                return res.status(400).json({
                    success: false,
                    message: "Report ID is required"
                });
            }

            const report = await AudioStreamingReportHistory.findById(id);

            if (!report) {
                return res.status(404).json({
                    success: false,
                    message: "Report not found"
                });
            }

            let fileDeleted = false;

            if (report.filePath) {
                // 🔑 FORCE src BASE
                const absolutePath = path.resolve(
                    process.cwd(),
                    'src',
                    report.filePath.replace(/^src[\\/]/, '')
                );

                console.log("Deleting file:", absolutePath);

                if (fs.existsSync(absolutePath)) {
                    fs.unlinkSync(absolutePath);
                    fileDeleted = true;
                }
            }

            await AudioStreamingReportHistory.findByIdAndDelete(id);

            return res.json({
                success: true,
                message: "Report and file deleted successfully",
                fileDeleted
            });

        } catch (error) {
            console.error("Delete error:", error);
            next(error);
        }
    }

    //getYoutubeReportHistory method
    async getYoutubeReportHistory(req, res, next) {
        try {
            const { role, userId } = req.user;

            const query = {};

            if (role !== "Super Admin" && role !== "Manager") {
                query.user_id = userId;
            }
            const reports = await YoutubeReportHistory.find(query)
                .select('filename filePath fileURL status generatedAt')
                .sort({ generatedAt: -1 })
                .lean();

            return res.status(200).json({
                success: true,
                count: reports.length,
                data: reports
            });

        } catch (error) {
            console.error("Error fetching report history:", error);

            return res.status(500).json({
                success: false,
                message: "Failed to fetch report history",
                error: error.message
            });
        }
    }

    //deleteYoutubeReportHistory method
    async deleteYoutubeReportHistory(req, res, next) {
        try {
            const { id } = req.query;

            if (!id) {
                return res.status(400).json({
                    success: false,
                    message: "Report ID is required"
                });
            }

            const report = await YoutubeReportHistory.findById(id);

            if (!report) {
                return res.status(404).json({
                    success: false,
                    message: "Report not found"
                });
            }

            let fileDeleted = false;

            if (report.filePath) {
                // 🔑 FORCE src BASE
                const absolutePath = path.resolve(
                    process.cwd(),
                    'src',
                    report.filePath.replace(/^src[\\/]/, '')
                );

                console.log("Deleting file:", absolutePath);

                if (fs.existsSync(absolutePath)) {
                    fs.unlinkSync(absolutePath);
                    fileDeleted = true;
                }
            }

            await YoutubeReportHistory.findByIdAndDelete(id);

            return res.json({
                success: true,
                message: "Report and file deleted successfully",
                fileDeleted
            });

        } catch (error) {
            console.error("Delete error:", error);
            next(error);
        }
    }

}

module.exports = new revenueUploadController();