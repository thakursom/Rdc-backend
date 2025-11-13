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
router.post("/add-user", authMiddleware, userController.addUser);

//Artist Apis
router.get("/fetchAllArtist", authMiddleware, artistController.getAllArtists);
router.get("/fetchArtistById", authMiddleware, artistController.fetchArtistById);
router.get("/fetchUserAndSubUsersArtist", authMiddleware, artistController.fetchUserAndSubUsersArtist);

//Third_party Apis
router.get("/fetch-and-store", authController.insertUsersFromAPI);
router.get("/fetch-artist", authController.insertArtistsFromAPI);


//Bank Details Apis
router.post("/addBankDetails", authMiddleware, bankController.addBankDetails);
router.get("/getBankDetails", authMiddleware, bankController.getBankDetails);
router.put("/editBankDetails/:id", authMiddleware, bankController.editBankDetails);
router.get("/getBankDetailById", authMiddleware, bankController.getBankDetailById);
router.delete("/deleteBankDetail/:id", authMiddleware, bankController.deleteBankDetail);


//Contract Apis
router.post("/addContract", upload.single("pdf"), authMiddleware, contractController.addContract);
router.put("/editContract/:id", upload.single("pdf"), authMiddleware, contractController.editContract);
router.get("/getAllContracts", authMiddleware, contractController.getAllContracts);
router.get("/getContractById", authMiddleware, contractController.getContractById);
router.delete("/deleteContract/:id", authMiddleware, contractController.deleteContract);
router.get("/contractLogs", authMiddleware, contractController.getContractLogs);
router.get("/getContractLogById", authMiddleware, contractController.getContractLogById);
router.post("/sendContractReminder/:id", authMiddleware, contractController.sendContractReminder);



module.exports = router;
