const express = require("express");
const router = express.Router();
const userController = require("../controllers/userController");
const authController = require("../controllers/authController");
const authMiddleware = require("../middlewares/authMiddleware");
const rbacMiddleware = require("../middlewares/rbacMiddleware");
const artistController = require("../controllers/artistController");
const bankController = require("../controllers/bankController");
const upload = require("../middlewares/upload");
const contractController = require("../controllers/contractController");
const conversionController = require("../controllers/conversionController");
const revenueController = require("../controllers/revenueUploadController");
const payoutController = require("../controllers/payoutController");
const logController = require("../controllers/logController");






//Auth Apis
router.post("/login", authController.login);
router.post("/forgot-password", authController.forgotPassword);
router.post("/reset-password/:token", authController.resetPassword);
router.post("/change-password", authMiddleware, authController.changePassword);

//User Apis
router.get("/fetchUser", authMiddleware, rbacMiddleware("user:view"), userController.getUsers);
router.get("/fetchAllUser", authMiddleware, userController.getAllUsers);
router.get("/fetchSubLabel", authMiddleware, userController.fetchSubLabel);
router.get("/fetchAllLabel", authMiddleware, userController.fetchAllLabels);
router.get("/fetchAllSubLabel", authMiddleware, userController.fetchAllSubLabel);
router.post("/add-user", authMiddleware, userController.addUser);
router.post("/uploadLabelAsUser", authMiddleware, upload.single("file"), userController.uploadLabelAsUser);

//Artist Apis
router.get("/fetchAllArtist", authMiddleware, artistController.getAllArtists);
router.get("/fetchArtistById", authMiddleware, artistController.fetchArtistById);
router.get("/fetchUserAndSubUsersArtist", authMiddleware, artistController.fetchUserAndSubUsersArtist);
router.get("/fetchArtistByName", authMiddleware, artistController.fetchArtistByName);

//Third_party Apis
router.get("/fetch-and-store", authController.insertUsersFromAPI);
router.get("/fetch-artist", authController.insertArtistsFromAPI);
router.get("/fetch-release", authController.insertReleasesFromAPI);
router.get("/fetch-track", authController.insertTracksFromAPI);


//Bank Details Apis
router.post("/addBankDetails", authMiddleware, bankController.addBankDetails);
router.get("/getBankDetails", authMiddleware, bankController.getBankDetails);
router.put("/editBankDetails/:id", authMiddleware, bankController.editBankDetails);
router.get("/getBankDetailById", authMiddleware, bankController.getBankDetailById);
router.delete("/deleteBankDetail/:id", authMiddleware, bankController.deleteBankDetail);
router.get("/getBankDetailByUserId", authMiddleware, bankController.getBankDetailByUserId);
router.get("/getBankDetailForPayout", authMiddleware, bankController.getBankDetailForPayout);


//Contract Apis
router.post("/addContract", upload.single("pdf"), authMiddleware, contractController.addContract);
router.put("/editContract/:id", upload.single("pdf"), authMiddleware, contractController.editContract);
router.get("/getAllContracts", authMiddleware, contractController.getAllContracts);
router.get("/getContractsByUser", authMiddleware, contractController.getContractsByUser);
router.get("/getContractById", authMiddleware, contractController.getContractById);
router.delete("/deleteContract/:id", authMiddleware, contractController.deleteContract);
router.get("/contractLogs", authMiddleware, contractController.getContractLogs);
router.get("/getContractLogById", authMiddleware, contractController.getContractLogById);
router.post("/sendContractReminder/:id", authMiddleware, contractController.sendContractReminder);
router.post("/sendContractWhatsappReminder/:id", authMiddleware, contractController.sendContractWhatsappReminder);
router.get("/fetchLabelAndSubLabelContract", authMiddleware, contractController.fetchLabelAndSubLabelContract);


//Xlsx Apis
router.post("/convert-xlsx-xml", authMiddleware, upload.single("file"), conversionController.convertXlsxToXml);

//Revenue Apis
router.post("/uploadRevenue", authMiddleware, upload.single("file"), revenueController.uploadRevenue);
router.get("/fetchAllRevenueUploads", authMiddleware, revenueController.getAllRevenueUploads);
router.get('/getRevenueById', authMiddleware, revenueController.getRevenueById);
router.post("/uploadTblRevenue", authMiddleware, revenueController.uploadTblRevenue);
router.get('/audioStreamingRevenueReport', authMiddleware, revenueController.getAudioStreamingRevenueReport);
router.get('/youtubeRevenueReport', authMiddleware, revenueController.getYoutubeRevenueReport);
// router.get('/revenueReports/export/audioStreamingExcel', authMiddleware, revenueController.downloadAudioStreamingExcelReport);
// router.get('/revenueReports/export/youtubeExcel', authMiddleware, revenueController.downloadYoutubeExcelReport);
router.delete('/deleteRevenueByUserId', authMiddleware, revenueController.deleteRevenueByUserId);
router.get('/report-history', authMiddleware, revenueController.getReportHistory);
router.delete('/delete-audio-report', authMiddleware, revenueController.deleteReportHistory);
router.get('/youtube-report-history', authMiddleware, revenueController.getYoutubeReportHistory);
router.delete('/delete-youtube-report', authMiddleware, revenueController.deleteYoutubeReportHistory);
router.get('/trigger-audio-streaming-excel', authMiddleware, revenueController.triggerAudioStreamingExcelReport);
router.get('/cron-audio-streaming-excel', revenueController.processPendingReports);
router.get('/trigger-youtube-excel', revenueController.triggerYoutubeExcelReport);


//Payout Apis
router.post("/createPayout", authMiddleware, payoutController.createPayout);
router.get("/getAllPayouts", authMiddleware, payoutController.getAllPayouts);
router.post("/uploadBulkPayout", authMiddleware, upload.single("file"), payoutController.uploadBulkPayout);

//Log Apis
router.get("/getAllLogs", authMiddleware, logController.getAllLogs);


module.exports = router;
