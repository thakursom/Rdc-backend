const XLSX = require("xlsx");

const LogService = require("../services/logService");
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




class revenueUploadController {

    constructor() { }


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

            const RevenueUploads = await RevenueUpload.create({
                user_id: 0,
                platform,
                periodFrom,
                periodTo,
                fileName: req.file.filename,
                filePath: fileURL,
                fileExt: req.file.mimetype,
            });

            // Read Excel
            const workbook = XLSX.readFile(req.file.path);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const jsonData = XLSX.utils.sheet_to_json(sheet);

            if (jsonData.length === 0) {
                return res.status(400).json({ error: "Excel file is empty" });
            }

            // const rows = jsonData.map(row => ({
            //     uploadId: RevenueUploads._id,
            //     ...row
            // }));

            const mappedRows = jsonData.map(r => {
                let obj = {};

                // FACEBOOK MAPPING
                if (platform === "Facebook") {
                    obj = {
                        retailer: r.service || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: r.elected_isrc || null,
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
                    };

                    // SPOTIFY MAPPING
                } else if (platform === "Spotify") {
                    obj = {
                        retailer: "Spotify",
                        label: r["Label Name"] || null,
                        upc_code: r.EAN || null,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
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
                    };

                    // Amazon MAPPING
                } else if (platform === "Amazon") {
                    obj = {
                        retailer: "Amazon",
                        label: r["Label Name"] || null,
                        upc_code: r["Digital Album Upc"] || null,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
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
                    };

                    //JioSaavan MAPPING
                } else if (platform === "JioSaavan") {
                    obj = {
                        retailer: "jio_savan",
                        label: r["Label Name"] || null,
                        upc_code: r.UPC || null,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
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
                    };

                    //AppleItunes MAPPING
                } else if (platform === "AppleItunes") {
                    obj = {
                        retailer: "Apple Music",
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
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
                    };

                    //TikTok MAPPING
                } else if (platform === "TikTok") {
                    obj = {
                        retailer: r.Platforn_Name,
                        label: r["Label Name"] || null,
                        upc_code: r.Product_Code,
                        catalogue_number: null,
                        isrc_code: r.Isrc || null,
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
                    };

                    //Gaana MAPPING
                } else if (platform === "Gaana") {
                    obj = {
                        retailer: "Gaana",
                        label: r["Label Name"] || null,
                        upc_code: r["Album UPC"],
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
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
                    };
                } else if (platform === "SoundRecording") {
                    obj = {
                        retailer: r["Channel Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: r.UPC,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
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
                    };
                } else if (platform === "YouTubeArtTrack") {
                    obj = {
                        retailer: r["Channel Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: r.UPC,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
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
                    };
                } else if (platform === "YouTubePartnerChannel") {
                    obj = {
                        retailer: r["Channel Display Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: null,
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
                    };
                } else if (platform === "YouTubeRDCChannel") {
                    obj = {
                        retailer: r["Channel Display Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: null,
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
                    };
                } else if (platform === "YouTubeVideoClaim") {
                    obj = {
                        retailer: r["Channel Display Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: null,
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
                    };
                } else if (platform === "YTPremiumRevenue") {
                    obj = {
                        retailer: r["Channel Display Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: null,
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
                    };
                }

                const today = new Date().toISOString().split("T")[0];
                obj.date = today;
                obj.user_id = 0;
                obj.uploading_date = today;
                obj.uploadId = RevenueUploads._id;

                return obj;
            });


            // Insert platform-specific revenue table
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
                return res.status(400).json({ success: false, message: "uploadId required" });
            }

            // Find and update the revenue upload
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

            //Get all TempReport rows for this uploadId
            const tempData = await TempReport.find({ uploadId }).lean();

            if (!tempData.length) {
                return res.status(404).json({ success: false, message: "No data found for this uploadId" });
            }

            // Remove MongoDB _id field so it can be inserted fresh
            const cleanedData = tempData.map(row => {
                const { _id, ...rest } = row;
                return rest;
            });

            //Insert into TblReport2025 in bulk
            await TblReport2025.insertMany(cleanedData);

            // Delete all existing data from TempReport
            await TempReport.deleteMany({ uploadId });

            await LogService.createLog({
                user_id: userId,
                email,
                action: `REVENUE_ADDED_IN_TBLREPORT_FOR_${tempData[0].retailer}`,
                description: `${tempData[0].retailer} revenue uploaded successfully in tbl_report`,
                newData: cleanedData,
                req
            });

            return res.status(200).json({
                success: true,
                message: "Data moved from TempReport to TblReport_2025 successfully",
                insertedCount: cleanedData.length
            });

        } catch (error) {
            console.error("uploadData error:", error);
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
                platform,
                month,
                quarter,
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
            const filter = {};

            if (platform && platform !== "") {
                const platforms = platform.split(",").map(p => p.trim());
                filter.retailer = { $in: platforms };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            // Month filter
            if (month && month !== '') {
                const year = new Date().getFullYear();
                const startDate = new Date(year, parseInt(month) - 1, 1);
                const endDate = new Date(year, parseInt(month), 0);
                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            // Quarter filter
            if (quarter && quarter !== '') {
                const quarterMonths = { '1': [1, 2, 3], '2': [4, 5, 6], '3': [7, 8, 9], '4': [10, 11, 12] };
                if (quarterMonths[quarter]) {
                    const year = new Date().getFullYear();
                    const months = quarterMonths[quarter];
                    const start = new Date(year, months[0] - 1, 1);
                    const end = new Date(year, months[2], 0);
                    filter.date = {
                        $gte: start.toISOString().split("T")[0],
                        $lte: end.toISOString().split("T")[0]
                    };
                }
            }

            // Custom date range
            if (fromDate && toDate) {
                filter.date = { $gte: fromDate, $lte: toDate };
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

            // Group by artist FIRST for consistent pagination
            const tablePipeline = [
                { $match: filter },
                addSafeRevenue,
                {
                    $group: {
                        _id: "$track_artist",
                        totalRevenue: { $sum: "$safeRevenue" },
                        firstDate: { $first: "$date" },
                        firstRetailer: { $first: "$retailer" },
                        firstRelease: { $first: "$release" },
                        artistName: { $first: "$track_artist" }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        date: "$firstDate",
                        platform: "$firstRetailer",
                        artist: "$artistName",
                        release: "$firstRelease",
                        revenue: { $round: ["$totalRevenue", 2] }
                    }
                },
                { $sort: { revenue: -1 } } // Sort by revenue descending
            ];

            const countPipeline = [
                { $match: filter },
                {
                    $group: {
                        _id: "$track_artist" // Count distinct artists
                    }
                },
                { $count: "total" }
            ];

            const paginatedPipeline = [
                ...tablePipeline,
                { $skip: (parseInt(page) - 1) * parseInt(limit) },
                { $limit: parseInt(limit) }
            ];

            const summaryPipeline = [
                { $match: filter },
                addSafeRevenue,
                {
                    $group: {
                        _id: null,
                        totalStreams: {
                            $sum: {
                                $convert: { input: "$track_count", to: "int", onError: 0, onNull: 0 }
                            }
                        },
                        totalRevenue: { $sum: "$safeRevenue" }
                    }
                }
            ];

            // 2. Revenue By Month (for stacked bar chart)
            const revenueByMonthPipeline = [
                { $match: filter },
                addSafeRevenue,
                {
                    $group: {
                        _id: {
                            year: { $year: { $dateFromString: { dateString: "$date" } } },
                            month: { $month: { $dateFromString: { dateString: "$date" } } }
                        },
                        revenue: { $sum: "$safeRevenue" }
                    }
                },
                {
                    $project: {
                        monthLabel: {
                            $dateToString: {
                                format: "%b %Y",
                                date: {
                                    $dateFromParts: {
                                        year: "$_id.year",
                                        month: "$_id.month",
                                        day: 1
                                    }
                                }
                            }
                        },
                        revenue: { $round: ["$revenue", 2] }
                    }
                },
                { $sort: { monthLabel: 1 } }
            ];

            // 3. Revenue By Channel (retailer)
            const revenueByChannelPipeline = [
                { $match: filter },
                addSafeRevenue,
                {
                    $group: {
                        _id: "$retailer",
                        revenue: { $sum: "$safeRevenue" }
                    }
                },
                {
                    $project: {
                        platform: "$_id",
                        revenue: { $round: ["$revenue", 2] },
                        _id: 0
                    }
                },
                { $sort: { revenue: -1 } }
            ];

            // 4. Revenue By Country
            const revenueByCountryPipeline = [
                { $match: filter },
                addSafeRevenue,
                {
                    $group: {
                        _id: "$territory",
                        revenue: { $sum: "$safeRevenue" }
                    }
                },
                {
                    $project: {
                        country: "$_id",
                        revenue: { $round: ["$revenue", 2] },
                        _id: 0
                    }
                },
                { $sort: { revenue: -1 } },
                { $limit: 10 } // Top 10 countries
            ];

            // Execute ALL in parallel
            const [
                paginatedData,
                countResult,
                summaryResult,
                byMonthResult,
                byChannelResult,
                byCountryResult
            ] = await Promise.all([
                TblReport2025.aggregate(paginatedPipeline),
                TblReport2025.aggregate(countPipeline),
                TblReport2025.aggregate(summaryPipeline),
                TblReport2025.aggregate(revenueByMonthPipeline),
                TblReport2025.aggregate(revenueByChannelPipeline),
                TblReport2025.aggregate(revenueByCountryPipeline)
            ]);

            const totalRecords = countResult[0]?.total || 0;
            const totalPages = Math.ceil(totalRecords / parseInt(limit));

            const summary = summaryResult[0] || { totalStreams: 0, totalRevenue: 0 };
            const revenueByChannel = {};
            defaultRetailers.forEach(platform => {
                const found = byChannelResult.find(item => item.platform === platform);
                revenueByChannel[platform] = found ? found.revenue : 0;
            });


            // Format response
            res.json({
                success: true,
                data: {
                    summary: {
                        totalStreams: summary.totalStreams,
                        totalRevenue: Number(summary.totalRevenue.toFixed(2))
                    },
                    reports: paginatedData,
                    pagination: {
                        totalRecords,
                        totalPages,
                        currentPage: parseInt(page),
                        limit: parseInt(limit)
                    },
                    revenueByMonth: Object.fromEntries(
                        byMonthResult.map(item => [item.monthLabel, item.revenue])
                    ),
                    revenueByChannel,
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
                platform,
                month,
                quarter,
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

            const defaultRetailers = [
                "Sound Recording (Audio Claim)",
                "Art Track (YouTube Music)",
                "YouTubePartnerChannel",
                "YouTubeRDCChannel",
                "YouTubeVideoClaim",
                "YTPremiumRevenue",
            ];

            // -------- BUILD FILTER --------
            const filter = {};

            if (platform && platform !== "") {
                const platforms = platform.split(",").map(p => p.trim());
                filter.retailer = { $in: platforms };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            // Month filter
            if (month && month !== '') {
                const year = new Date().getFullYear();
                const startDate = new Date(year, parseInt(month) - 1, 1);
                const endDate = new Date(year, parseInt(month), 0);
                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            // Quarter filter
            if (quarter && quarter !== '') {
                const quarterMonths = { '1': [1, 2, 3], '2': [4, 5, 6], '3': [7, 8, 9], '4': [10, 11, 12] };
                if (quarterMonths[quarter]) {
                    const year = new Date().getFullYear();
                    const months = quarterMonths[quarter];
                    const start = new Date(year, months[0] - 1, 1);
                    const end = new Date(year, months[2], 0);
                    filter.date = {
                        $gte: start.toISOString().split("T")[0],
                        $lte: end.toISOString().split("T")[0]
                    };
                }
            }

            // Custom date range
            if (fromDate && toDate) {
                filter.date = { $gte: fromDate, $lte: toDate };
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

            // Group by artist FIRST for consistent pagination
            const tablePipeline = [
                { $match: filter },
                addSafeRevenue,
                {
                    $group: {
                        _id: "$track_artist",
                        totalRevenue: { $sum: "$safeRevenue" },
                        firstDate: { $first: "$date" },
                        firstRetailer: { $first: "$retailer" },
                        firstRelease: { $first: "$release" },
                        artistName: { $first: "$track_artist" }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        date: "$firstDate",
                        platform: "$firstRetailer",
                        artist: "$artistName",
                        release: "$firstRelease",
                        revenue: { $round: ["$totalRevenue", 2] }
                    }
                },
                { $sort: { revenue: -1 } } // Sort by revenue descending
            ];

            const countPipeline = [
                { $match: filter },
                {
                    $group: {
                        _id: "$track_artist" // Count distinct artists
                    }
                },
                { $count: "total" }
            ];

            const paginatedPipeline = [
                ...tablePipeline,
                { $skip: (parseInt(page) - 1) * parseInt(limit) },
                { $limit: parseInt(limit) }
            ];

            const summaryPipeline = [
                { $match: filter },
                addSafeRevenue,
                {
                    $group: {
                        _id: null,
                        totalStreams: {
                            $sum: {
                                $convert: { input: "$track_count", to: "int", onError: 0, onNull: 0 }
                            }
                        },
                        totalRevenue: { $sum: "$safeRevenue" }
                    }
                }
            ];

            // 2. Revenue By Month (for stacked bar chart)
            const revenueByMonthPipeline = [
                { $match: filter },
                addSafeRevenue,
                {
                    $group: {
                        _id: {
                            year: { $year: { $dateFromString: { dateString: "$date" } } },
                            month: { $month: { $dateFromString: { dateString: "$date" } } }
                        },
                        revenue: { $sum: "$safeRevenue" }
                    }
                },
                {
                    $project: {
                        monthLabel: {
                            $dateToString: {
                                format: "%b %Y",
                                date: {
                                    $dateFromParts: {
                                        year: "$_id.year",
                                        month: "$_id.month",
                                        day: 1
                                    }
                                }
                            }
                        },
                        revenue: { $round: ["$revenue", 2] }
                    }
                },
                { $sort: { monthLabel: 1 } }
            ];

            // 3. Revenue By Channel (retailer)
            const revenueByChannelPipeline = [
                { $match: filter },
                addSafeRevenue,
                {
                    $group: {
                        _id: "$retailer",
                        revenue: { $sum: "$safeRevenue" }
                    }
                },
                {
                    $project: {
                        platform: "$_id",
                        revenue: { $round: ["$revenue", 2] },
                        _id: 0
                    }
                },
                { $sort: { revenue: -1 } }
            ];

            // 4. Revenue By Country
            const revenueByCountryPipeline = [
                { $match: filter },
                addSafeRevenue,
                {
                    $group: {
                        _id: "$territory",
                        revenue: { $sum: "$safeRevenue" }
                    }
                },
                {
                    $project: {
                        country: "$_id",
                        revenue: { $round: ["$revenue", 2] },
                        _id: 0
                    }
                },
                { $sort: { revenue: -1 } },
                { $limit: 10 } // Top 10 countries
            ];

            // Execute ALL in parallel
            const [
                paginatedData,
                countResult,
                summaryResult,
                byMonthResult,
                byChannelResult,
                byCountryResult
            ] = await Promise.all([
                TblReport2025.aggregate(paginatedPipeline),
                TblReport2025.aggregate(countPipeline),
                TblReport2025.aggregate(summaryPipeline),
                TblReport2025.aggregate(revenueByMonthPipeline),
                TblReport2025.aggregate(revenueByChannelPipeline),
                TblReport2025.aggregate(revenueByCountryPipeline)
            ]);

            const totalRecords = countResult[0]?.total || 0;
            const totalPages = Math.ceil(totalRecords / parseInt(limit));

            const summary = summaryResult[0] || { totalStreams: 0, totalRevenue: 0 };
            const revenueByChannel = {};
            defaultRetailers.forEach(platform => {
                const found = byChannelResult.find(item => item.platform === platform);
                revenueByChannel[platform] = found ? found.revenue : 0;
            });


            // Format response
            res.json({
                success: true,
                data: {
                    summary: {
                        totalStreams: summary.totalStreams,
                        totalRevenue: Number(summary.totalRevenue.toFixed(2))
                    },
                    reports: paginatedData,
                    pagination: {
                        totalRecords,
                        totalPages,
                        currentPage: parseInt(page),
                        limit: parseInt(limit)
                    },
                    revenueByMonth: Object.fromEntries(
                        byMonthResult.map(item => [item.monthLabel, item.revenue])
                    ),
                    revenueByChannel,
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
    async downloadAudioStreamingExcelReport(req, res, next) {
        try {
            const {
                platform,
                month,
                quarter,
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
            } = req.query;


            const defaultRetailers = [
                "Apple Music",
                "Spotify",
                "Gaana",
                "Jio Saavn",
                "Facebook",
                "Amazon",
                "TikTok"
            ];

            const filter = {};

            if (platform && platform !== "") {
                const platforms = platform.split(",").map(p => p.trim());
                filter.retailer = { $in: platforms };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            // Month filter
            if (month && month !== '') {
                const year = new Date().getFullYear();
                const startDate = new Date(year, parseInt(month) - 1, 1);
                const endDate = new Date(year, parseInt(month), 0);

                filter.date = {
                    $gte: startDate.toISOString().split('T')[0],
                    $lte: endDate.toISOString().split('T')[0]
                };
            }

            // Quarter filter
            if (quarter && quarter !== '') {
                const quarterMonths = {
                    '1': [1, 2, 3],
                    '2': [4, 5, 6],
                    '3': [7, 8, 9],
                    '4': [10, 11, 12]
                };

                if (quarterMonths[quarter]) {
                    const year = new Date().getFullYear();
                    const months = quarterMonths[quarter];
                    const start = new Date(year, months[0] - 1, 1);
                    const end = new Date(year, months[2], 0);

                    filter.date = {
                        $gte: start.toISOString().split('T')[0],
                        $lte: end.toISOString().split('T')[0]
                    };
                }
            }

            // Custom date range
            if (fromDate && toDate) {
                filter.date = { $gte: fromDate, $lte: toDate };
            }

            // Checkbox filters
            if (artist === "true") filter.track_artist = { $nin: ["", null, undefined] };
            if (territory === "true") filter.territory = { $nin: ["", null, undefined] };
            if (releases === "true") filter.release = { $nin: ["", null, undefined] };

            // Get data WITHOUT aggregation for simplicity
            const data = await TblReport2025.find(filter)
                // .select('date retailer track_artist release track_count net_total')
                .select('-__v')
                .sort({ date: -1 })
                .lean();

            console.log(`Found ${data.length} records for export`);

            if (data.length === 0) {
                // Send a proper error response
                return res.status(404).json({
                    success: false,
                    message: "No data found to export"
                });
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

            // Generate filename - SIMPLIFY it
            const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
            const filename = `Revenue_Report_${timestamp}.xlsx`;

            console.log(`Creating Excel file: ${filename}`);

            // **FIX: Write to buffer with proper options**
            const excelBuffer = XLSX.write(workbook, {
                type: 'buffer',
                bookType: 'xlsx',
                bookSST: false
            });

            console.log(`Excel buffer size: ${excelBuffer.length} bytes`);

            // **FIX: Set headers CORRECTLY**
            res.writeHead(200, {
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'Content-Disposition': `attachment; filename="${filename}"`,
                'Content-Length': excelBuffer.length,
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache',
                'Expires': '0'
            });

            // **FIX: Send the buffer**
            res.end(excelBuffer);
            console.log('=== Excel file sent successfully ===');

        } catch (error) {
            console.error("Error in downloadExcelReport:", error);

            // Send JSON error if headers not sent yet
            if (!res.headersSent) {
                res.status(500).json({
                    success: false,
                    message: "Error generating Excel file",
                    error: error.message
                });
            }
        }
    }

    // downloadYoutubeExcelReport method
    async downloadYoutubeExcelReport(req, res, next) {

        try {
            const {
                platform,
                month,
                quarter,
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
            } = req.query;


            const defaultRetailers = [
                "Sound Recording (Audio Claim)",
                "Art Track (YouTube Music)",
                "YouTubePartnerChannel",
                "YouTubeRDCChannel",
                "YouTubeVideoClaim",
                "YTPremiumRevenue",
            ];

            const filter = {};

            if (platform && platform !== "") {
                const platforms = platform.split(",").map(p => p.trim());
                filter.retailer = { $in: platforms };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            // Month filter
            if (month && month !== '') {
                const year = new Date().getFullYear();
                const startDate = new Date(year, parseInt(month) - 1, 1);
                const endDate = new Date(year, parseInt(month), 0);

                filter.date = {
                    $gte: startDate.toISOString().split('T')[0],
                    $lte: endDate.toISOString().split('T')[0]
                };
            }

            // Quarter filter
            if (quarter && quarter !== '') {
                const quarterMonths = {
                    '1': [1, 2, 3],
                    '2': [4, 5, 6],
                    '3': [7, 8, 9],
                    '4': [10, 11, 12]
                };

                if (quarterMonths[quarter]) {
                    const year = new Date().getFullYear();
                    const months = quarterMonths[quarter];
                    const start = new Date(year, months[0] - 1, 1);
                    const end = new Date(year, months[2], 0);

                    filter.date = {
                        $gte: start.toISOString().split('T')[0],
                        $lte: end.toISOString().split('T')[0]
                    };
                }
            }

            // Custom date range
            if (fromDate && toDate) {
                filter.date = { $gte: fromDate, $lte: toDate };
            }

            // Checkbox filters
            if (artist === "true") filter.track_artist = { $nin: ["", null, undefined] };
            if (territory === "true") filter.territory = { $nin: ["", null, undefined] };
            if (releases === "true") filter.release = { $nin: ["", null, undefined] };

            // Get data WITHOUT aggregation for simplicity
            const data = await TblReport2025.find(filter)
                // .select('date retailer track_artist release track_count net_total')
                .select('-__v')
                .sort({ date: -1 })
                .lean();

            console.log(`Found ${data.length} records for export`);

            if (data.length === 0) {
                // Send a proper error response
                return res.status(404).json({
                    success: false,
                    message: "No data found to export"
                });
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

            // Generate filename - SIMPLIFY it
            const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
            const filename = `Revenue_Report_${timestamp}.xlsx`;

            console.log(`Creating Excel file: ${filename}`);

            // **FIX: Write to buffer with proper options**
            const excelBuffer = XLSX.write(workbook, {
                type: 'buffer',
                bookType: 'xlsx',
                bookSST: false
            });

            console.log(`Excel buffer size: ${excelBuffer.length} bytes`);

            // **FIX: Set headers CORRECTLY**
            res.writeHead(200, {
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'Content-Disposition': `attachment; filename="${filename}"`,
                'Content-Length': excelBuffer.length,
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache',
                'Expires': '0'
            });

            // **FIX: Send the buffer**
            res.end(excelBuffer);
            console.log('=== Excel file sent successfully ===');

        } catch (error) {
            console.error("Error in downloadExcelReport:", error);

            // Send JSON error if headers not sent yet
            if (!res.headersSent) {
                res.status(500).json({
                    success: false,
                    message: "Error generating Excel file",
                    error: error.message
                });
            }
        }
    }


}

module.exports = new revenueUploadController();