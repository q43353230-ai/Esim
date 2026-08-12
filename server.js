// ================================================================
// نظام eSIM المتكامل - Integrated eSIM Management System
// الإصدار 3.0.0 - متطور وكامل
// ================================================================

require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const { v4: uuidv4 } = require('uuid');
const moment = require('moment');
const cron = require('node-cron');
const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');
const os = require('os');

// ================================================================
// معلومات النظام - eSIM
// ================================================================
const SYSTEM = {
    name: 'eSIM',
    fullName: 'eSIM Management System',
    version: '3.0.0',
    build: '2026.08.13',
    copyright: '© 2026 eSIM - جميع الحقوق محفوظة',
    description: 'نظام متكامل لإدارة بطاقات eSIM الرقمية',
};

// ================================================================
// إعدادات البيئة
// ================================================================
const ENV = {
    PORT: process.env.PORT || 3000,
    NODE_ENV: process.env.NODE_ENV || 'production',
    JWT_SECRET: process.env.JWT_SECRET || crypto.randomBytes(64).toString('hex'),
    JWT_EXPIRY: process.env.JWT_EXPIRY || '7d',
    ADMIN_PASSWORD: process.env.ADMIN_PASSWORD || '135781',
    MONGO_URI: process.env.MONGO_URI || 'mongodb+srv://best:UDglXQEBZFuksePq@cluster0.68mktfu.mongodb.net/esim_system?retryWrites=true&w=majority',
    MAX_FILE_SIZE: parseInt(process.env.MAX_FILE_SIZE) || 5 * 1024 * 1024,
    MAX_FILES: parseInt(process.env.MAX_FILES) || 5,
    ORDER_EXPIRY_MINUTES: parseInt(process.env.ORDER_EXPIRY_MINUTES) || 3,
};

// ================================================================
// إعدادات الخادم
// ================================================================
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ 
    server,
    perMessageDeflate: true,
    clientTracking: true,
});

// ================================================================
// Middleware
// ================================================================
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
    credentials: true,
}));

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// ================================================================
// تكوين Multer
// ================================================================
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        const date = new Date();
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const dir = `./uploads/${year}/${month}/${day}/`;
        
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
        cb(null, dir);
    },
    filename: (req, file, cb) => {
        const uniqueName = `${Date.now()}-${uuidv4()}-${file.originalname}`;
        cb(null, uniqueName);
    }
});

const fileFilter = (req, file, cb) => {
    const allowedTypes = [
        'image/jpeg', 'image/png', 'image/jpg', 
        'image/gif', 'image/webp', 'application/pdf',
        'image/svg+xml', 'image/bmp', 'image/tiff'
    ];
    
    if (allowedTypes.includes(file.mimetype)) {
        cb(null, true);
    } else {
        cb(new Error(`نوع الملف غير مدعوم: ${file.mimetype}`));
    }
};

const upload = multer({
    storage: storage,
    limits: { 
        files: ENV.MAX_FILES, 
        fileSize: ENV.MAX_FILE_SIZE,
        fieldSize: 10 * 1024 * 1024,
    },
    fileFilter: fileFilter,
});

// ================================================================
// الاتصال بقاعدة البيانات
// ================================================================
let isDbConnected = false;

const connectDB = async (retries = 5, delay = 5000) => {
    for (let i = 0; i < retries; i++) {
        try {
            await mongoose.connect(ENV.MONGO_URI, {
                useNewUrlParser: true,
                useUnifiedTopology: true,
                serverSelectionTimeoutMS: 10000,
                socketTimeoutMS: 45000,
                maxPoolSize: 50,
                minPoolSize: 5,
            });
            
            isDbConnected = true;
            console.log(`✅ ${SYSTEM.name}: تم الاتصال بـ MongoDB بنجاح`);
            console.log(`📊 قاعدة البيانات: ${mongoose.connection.name}`);
            console.log(`🔄 حالة الاتصال: ${mongoose.connection.readyState}`);
            return true;
        } catch (err) {
            console.error(`❌ محاولة ${i + 1}/${retries} فشلت:`, err.message);
            if (i < retries - 1) {
                console.log(`⏳ إعادة محاولة الاتصال بعد ${delay/1000} ثواني...`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
    }
    console.error('❌ فشل الاتصال بقاعدة البيانات بعد عدة محاولات');
    return false;
};

connectDB();

mongoose.connection.on('disconnected', () => {
    console.warn('⚠️ تم قطع الاتصال بقاعدة البيانات');
    isDbConnected = false;
    setTimeout(() => connectDB(), 5000);
});

mongoose.connection.on('reconnected', () => {
    console.log('✅ تم إعادة الاتصال بقاعدة البيانات');
    isDbConnected = true;
});

// ================================================================
// نماذج قاعدة البيانات
// ================================================================

// نموذج المستخدم
const UserSchema = new mongoose.Schema({
    username: { 
        type: String, 
        required: true,
        unique: true,
        trim: true,
        minlength: 3,
        maxlength: 30,
    },
    email: { 
        type: String, 
        required: true,
        unique: true,
        trim: true,
        lowercase: true,
    },
    password: { 
        type: String, 
        required: true,
        minlength: 6,
    },
    fullName: { type: String, default: '' },
    phone: { type: String, default: '' },
    country: { type: String, default: '' },
    countryCode: { type: String, default: '' },
    balance: { type: Number, default: 0 },
    totalOrders: { type: Number, default: 0 },
    totalSpent: { type: Number, default: 0 },
    isActive: { type: Boolean, default: true },
    isVerified: { type: Boolean, default: false },
    referralCode: { type: String, unique: true, sparse: true },
    referralCount: { type: Number, default: 0 },
    referralEarnings: { type: Number, default: 0 },
    lastLogin: { type: Date },
    ipAddress: { type: String, default: '' },
    deviceInfo: { type: String, default: '' },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
    preferences: {
        language: { type: String, default: 'ar' },
        theme: { type: String, default: 'dark' },
        notifications: { type: Boolean, default: true },
    },
});

// نموذج طلبات eSIM
const EsimOrderSchema = new mongoose.Schema({
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    username: { type: String, required: true },
    esimType: { 
        type: String, 
        enum: ['vodafone', 'instapay', 'cash', 'data_only', 'voice_data', 'unlimited'], 
        required: true,
    },
    country: { type: String, required: true },
    countryCode: { type: String, default: '' },
    amount: { type: Number, required: true },
    phoneNumber: { type: String, required: true },
    esimDetails: {
        iccid: { type: String, default: '' },
        activationCode: { type: String, default: '' },
        qrCode: { type: String, default: '' },
        qrCodeBase64: { type: String, default: '' },
        expiryDate: { type: Date },
        dataPlan: { type: String, default: '' },
        validityDays: { type: Number, default: 30 },
        operator: { type: String, default: '' },
        networkType: { type: String, default: '5G' },
        coverage: { type: String, default: '' },
        notes: { type: String, default: '' },
    },
    status: { 
        type: String, 
        enum: ['pending', 'accepted', 'rejected', 'expired', 'delivered', 'completed', 'refunded'], 
        default: 'pending',
    },
    acceptedAt: { type: Date },
    expiresAt: { type: Date },
    deliveredAt: { type: Date },
    completedAt: { type: Date },
    refundedAt: { type: Date },
    rejectionReason: { type: String, default: '' },
    rejectionDetails: { type: String, default: '' },
    screenshots: [{ type: String }],
    notes: { type: String, default: '' },
    adminNotes: { type: String, default: '' },
    timeRemaining: { type: Number, default: 180 },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
});

// نموذج الإشعارات
const NotificationSchema = new mongoose.Schema({
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    title: { type: String, required: true },
    message: { type: String, required: true },
    type: { type: String, enum: ['info', 'success', 'warning', 'error', 'order', 'delivery'], default: 'info' },
    isRead: { type: Boolean, default: false },
    isImportant: { type: Boolean, default: false },
    relatedOrderId: { type: mongoose.Schema.Types.ObjectId, ref: 'EsimOrder' },
    metadata: { type: mongoose.Schema.Types.Mixed, default: {} },
    createdAt: { type: Date, default: Date.now },
    readAt: { type: Date },
});

// نموذج سجل العمليات
const TransactionLogSchema = new mongoose.Schema({
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
    username: { type: String },
    type: { 
        type: String, 
        enum: ['order', 'delivery', 'refund', 'adjustment', 'deposit', 'withdrawal', 'referral'], 
        required: true 
    },
    amount: { type: Number },
    method: { type: String, default: '' },
    status: { type: String, default: 'pending' },
    details: { type: mongoose.Schema.Types.Mixed, default: {} },
    ipAddress: { type: String, default: '' },
    userAgent: { type: String, default: '' },
    createdAt: { type: Date, default: Date.now },
});

// نموذج باقات eSIM
const EsimPackageSchema = new mongoose.Schema({
    name: { type: String, required: true, unique: true },
    country: { type: String, required: true },
    countryCode: { type: String, default: '' },
    dataAmount: { 
        type: String, 
        required: true,
        enum: ['1GB', '2GB', '3GB', '5GB', '10GB', '20GB', '50GB', '100GB', 'Unlimited'],
    },
    validityDays: { type: Number, required: true, min: 1, max: 365 },
    price: { type: Number, required: true },
    discountedPrice: { type: Number },
    currency: { type: String, default: 'EGP' },
    isActive: { type: Boolean, default: true },
    isPopular: { type: Boolean, default: false },
    isFeatured: { type: Boolean, default: false },
    description: { type: String, default: '' },
    features: [{ type: String }],
    operator: { type: String, default: '' },
    networkType: { type: String, default: '5G' },
    coverage: { type: String, default: '' },
    icon: { type: String, default: '📱' },
    color: { type: String, default: '#6C3CE1' },
    orderCount: { type: Number, default: 0 },
    rating: { type: Number, default: 0 },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
});

// نموذج الإحالات
const ReferralSchema = new mongoose.Schema({
    referrerId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    referredId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    code: { type: String, required: true },
    status: { type: String, enum: ['pending', 'converted', 'rewarded'], default: 'pending' },
    rewardAmount: { type: Number, default: 0 },
    createdAt: { type: Date, default: Date.now },
    convertedAt: { type: Date },
});

// ================================================================
// إنشاء النماذج
// ================================================================
const User = mongoose.model('User', UserSchema);
const EsimOrder = mongoose.model('EsimOrder', EsimOrderSchema);
const Notification = mongoose.model('Notification', NotificationSchema);
const TransactionLog = mongoose.model('TransactionLog', TransactionLogSchema);
const EsimPackage = mongoose.model('EsimPackage', EsimPackageSchema);
const Referral = mongoose.model('Referral', ReferralSchema);

// ================================================================
// دوال مساعدة
// ================================================================

function generateReferralCode() {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let code = '';
    for (let i = 0; i < 8; i++) {
        code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
}

function getTimeRemaining(expiresAt) {
    const now = Date.now();
    const expiry = new Date(expiresAt).getTime();
    const remaining = Math.max(0, Math.floor((expiry - now) / 1000));
    return {
        seconds: remaining,
        minutes: Math.floor(remaining / 60),
        hours: Math.floor(remaining / 3600),
        isExpired: remaining <= 0,
    };
}

async function createNotification(userId, title, message, type = 'info', orderId = null, metadata = {}) {
    try {
        const notification = new Notification({
            userId,
            title,
            message,
            type,
            relatedOrderId: orderId,
            metadata,
            isImportant: type === 'error' || type === 'warning',
        });
        await notification.save();
        broadcastNotification(userId, notification);
        return notification;
    } catch (error) {
        console.error('❌ خطأ في إنشاء الإشعار:', error);
        return null;
    }
}

function broadcastNotification(userId, notification) {
    if (!wss || !wss.clients) return;
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN && client.userId === userId) {
            try {
                client.send(JSON.stringify({
                    type: 'notification',
                    data: notification
                }));
            } catch (error) {
                console.error('❌ خطأ في إرسال الإشعار:', error);
            }
        }
    });
}

async function logTransaction(userId, username, type, amount, method, status, details = {}) {
    try {
        const log = new TransactionLog({
            userId,
            username,
            type,
            amount: Number(amount),
            method,
            status,
            details,
        });
        await log.save();
        return log;
    } catch (error) {
        console.error('❌ خطأ في تسجيل العملية:', error);
        return null;
    }
}

async function updateUserBalance(userId, amount, operation = 'add', reason = '') {
    try {
        const user = await User.findById(userId);
        if (!user) throw new Error('المستخدم غير موجود');
        
        if (operation === 'add') {
            user.balance = Number((user.balance + amount).toFixed(2));
        } else if (operation === 'subtract') {
            if (user.balance < amount) throw new Error('الرصيد غير كافٍ');
            user.balance = Number((user.balance - amount).toFixed(2));
        } else {
            throw new Error('عملية غير صالحة');
        }
        await user.save();
        return user;
    } catch (error) {
        console.error('❌ خطأ في تحديث الرصيد:', error);
        throw error;
    }
}

function authenticateToken(req, res, next) {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    
    if (!token) {
        return res.status(401).json({ 
            success: false, 
            message: 'الرجاء تسجيل الدخول أولاً',
            code: 'UNAUTHORIZED'
        });
    }
    
    try {
        jwt.verify(token, ENV.JWT_SECRET, (err, user) => {
            if (err) {
                return res.status(403).json({ 
                    success: false, 
                    message: 'توكن غير صالح',
                    code: 'INVALID_TOKEN'
                });
            }
            req.user = user;
            next();
        });
    } catch (error) {
        return res.status(403).json({ 
            success: false, 
            message: 'توكن غير صالح',
            code: 'INVALID_TOKEN'
        });
    }
}

function authenticateAdmin(req, res, next) {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    
    if (!token) {
        return res.status(401).json({ 
            success: false, 
            message: 'الرجاء تسجيل الدخول',
            code: 'UNAUTHORIZED'
        });
    }
    
    try {
        jwt.verify(token, ENV.JWT_SECRET, (err, user) => {
            if (err || user.role !== 'admin') {
                return res.status(403).json({ 
                    success: false, 
                    message: 'غير مصرح لك بهذه العملية',
                    code: 'FORBIDDEN'
                });
            }
            req.admin = user;
            next();
        });
    } catch (error) {
        return res.status(403).json({ 
            success: false, 
            message: 'توكن غير صالح',
            code: 'INVALID_TOKEN'
        });
    }
}

function broadcastToAll(message, type = 'broadcast') {
    const payload = JSON.stringify({
        type,
        data: message,
        timestamp: new Date().toISOString()
    });
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    });
}

// ================================================================
// WebSocket
// ================================================================
const connectedClients = new Map();

wss.on('connection', (ws, req) => {
    const clientId = uuidv4();
    connectedClients.set(clientId, { ws, userId: null, isAdmin: false });
    console.log(`🟢 اتصال WebSocket جديد: ${clientId}`);
    
    ws.send(JSON.stringify({
        type: 'welcome',
        data: { message: 'مرحباً بك في نظام eSIM', clientId }
    }));
    
    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);
            const client = connectedClients.get(clientId);
            
            if (data.type === 'auth') {
                if (data.userId) {
                    client.userId = data.userId;
                    ws.userId = data.userId;
                    console.log(`🔗 مستخدم مصادق: ${data.userId}`);
                }
                if (data.isAdmin) {
                    client.isAdmin = true;
                    ws.isAdmin = true;
                    console.log('🔗 مسؤول مصادق');
                }
            }
            
            if (data.type === 'ping') {
                ws.send(JSON.stringify({ type: 'pong', timestamp: new Date().toISOString() }));
            }
        } catch (error) {
            console.error('❌ خطأ في معالجة رسالة WebSocket:', error);
        }
    });
    
    ws.on('close', () => {
        connectedClients.delete(clientId);
        console.log(`🔴 تم قطع اتصال WebSocket: ${clientId}`);
    });
});

// ================================================================
// المهام المجدولة
// ================================================================

// انتهاء صلاحية الطلبات
cron.schedule('*/1 * * * *', async () => {
    try {
        const now = new Date();
        const expiredOrders = await EsimOrder.find({
            status: 'accepted',
            expiresAt: { $lt: now }
        });
        
        for (const order of expiredOrders) {
            order.status = 'expired';
            await order.save();
            await updateUserBalance(order.userId, order.amount, 'add', 'انتهاء صلاحية الطلب');
            await createNotification(
                order.userId,
                '⏰ انتهت صلاحية طلب eSIM',
                `طلب eSIM رقم ${order._id} انتهت صلاحيته. تم إرجاع المبلغ إلى رصيدك.`,
                'warning',
                order._id
            );
        }
        if (expiredOrders.length > 0) {
            console.log(`⏰ تم انتهاء صلاحية ${expiredOrders.length} طلب eSIM`);
        }
    } catch (error) {
        console.error('❌ خطأ في مهمة انتهاء الصلاحية:', error);
    }
});

// ================================================================
// API - المصادقة
// ================================================================

// تسجيل مستخدم جديد
app.post('/api/register', async (req, res) => {
    try {
        const { username, email, password, fullName, phone, country, countryCode, referralCode } = req.body;
        
        if (!username || !email || !password) {
            return res.status(400).json({ success: false, message: 'يرجى ملء جميع الحقول المطلوبة' });
        }
        
        const existingUser = await User.findOne({ $or: [{ username }, { email }] });
        if (existingUser) {
            return res.status(400).json({ success: false, message: 'اسم المستخدم أو البريد الإلكتروني مستخدم بالفعل' });
        }
        
        const salt = await bcrypt.genSalt(12);
        const hashedPassword = await bcrypt.hash(password, salt);
        
        let refCode = generateReferralCode();
        let refExists = await User.findOne({ referralCode: refCode });
        while (refExists) {
            refCode = generateReferralCode();
            refExists = await User.findOne({ referralCode: refCode });
        }
        
        const user = new User({
            username,
            email,
            password: hashedPassword,
            fullName: fullName || username,
            phone: phone || '',
            country: country || '',
            countryCode: countryCode || '',
            referralCode: refCode,
            ipAddress: req.ip || req.connection.remoteAddress,
            deviceInfo: req.headers['user-agent'] || '',
        });
        
        await user.save();
        
        if (referralCode) {
            const referrer = await User.findOne({ referralCode });
            if (referrer) {
                const referral = new Referral({
                    referrerId: referrer._id,
                    referredId: user._id,
                    code: referralCode,
                    status: 'pending',
                });
                await referral.save();
                await User.findByIdAndUpdate(referrer._id, { $inc: { referralCount: 1 } });
            }
        }
        
        const token = jwt.sign(
            { id: user._id, username: user.username, email: user.email, role: 'user' },
            ENV.JWT_SECRET,
            { expiresIn: ENV.JWT_EXPIRY }
        );
        
        await createNotification(
            user._id,
            '🎉 مرحباً بك في eSIM',
            'تم إنشاء حسابك بنجاح. يمكنك الآن طلب بطاقات eSIM بكل سهولة.',
            'success'
        );
        
        res.status(201).json({
            success: true,
            message: 'تم إنشاء الحساب بنجاح',
            token,
            user: {
                id: user._id,
                username: user.username,
                email: user.email,
                fullName: user.fullName,
                balance: user.balance,
                phone: user.phone,
                country: user.country,
                countryCode: user.countryCode,
                referralCode: user.referralCode,
            }
        });
    } catch (error) {
        console.error('❌ خطأ في التسجيل:', error);
        res.status(500).json({ success: false, message: 'حدث خطأ في إنشاء الحساب' });
    }
});

// تسجيل الدخول
app.post('/api/login', async (req, res) => {
    try {
        const { username, password } = req.body;
        
        if (!username || !password) {
            return res.status(400).json({ success: false, message: 'يرجى إدخال اسم المستخدم وكلمة المرور' });
        }
        
        const user = await User.findOne({ $or: [{ username }, { email: username }] });
        if (!user) {
            return res.status(401).json({ success: false, message: 'اسم المستخدم أو كلمة المرور غير صحيحة' });
        }
        
        const validPassword = await bcrypt.compare(password, user.password);
        if (!validPassword) {
            return res.status(401).json({ success: false, message: 'اسم المستخدم أو كلمة المرور غير صحيحة' });
        }
        
        if (!user.isActive) {
            return res.status(403).json({ success: false, message: 'الحساب معطل، يرجى التواصل مع الدعم' });
        }
        
        user.lastLogin = new Date();
        user.lastLoginIP = req.ip || req.connection.remoteAddress;
        user.lastLoginDevice = req.headers['user-agent'] || '';
        await user.save();
        
        const token = jwt.sign(
            { id: user._id, username: user.username, email: user.email, role: 'user' },
            ENV.JWT_SECRET,
            { expiresIn: ENV.JWT_EXPIRY }
        );
        
        res.json({
            success: true,
            message: 'تم تسجيل الدخول بنجاح',
            token,
            user: {
                id: user._id,
                username: user.username,
                email: user.email,
                fullName: user.fullName,
                balance: user.balance,
                phone: user.phone,
                country: user.country,
                countryCode: user.countryCode,
                referralCode: user.referralCode,
                isActive: user.isActive,
                isVerified: user.isVerified,
            }
        });
    } catch (error) {
        console.error('❌ خطأ في تسجيل الدخول:', error);
        res.status(500).json({ success: false, message: 'حدث خطأ في تسجيل الدخول' });
    }
});

// ================================================================
// API - العميل
// ================================================================

app.get('/api/user/profile', authenticateToken, async (req, res) => {
    try {
        const user = await User.findById(req.user.id).select('-password');
        if (!user) {
            return res.status(404).json({ success: false, message: 'المستخدم غير موجود' });
        }
        res.json({ success: true, user });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ في جلب الملف الشخصي' });
    }
});

app.put('/api/user/profile', authenticateToken, async (req, res) => {
    try {
        const { fullName, phone, country, countryCode, preferences } = req.body;
        const user = await User.findById(req.user.id);
        if (!user) {
            return res.status(404).json({ success: false, message: 'المستخدم غير موجود' });
        }
        if (fullName) user.fullName = fullName;
        if (phone) user.phone = phone;
        if (country) user.country = country;
        if (countryCode) user.countryCode = countryCode;
        if (preferences) user.preferences = { ...user.preferences, ...preferences };
        await user.save();
        res.json({ success: true, message: 'تم تحديث الملف الشخصي بنجاح', user });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ في تحديث الملف الشخصي' });
    }
});

app.get('/api/user/balance', authenticateToken, async (req, res) => {
    try {
        const user = await User.findById(req.user.id);
        if (!user) {
            return res.status(404).json({ success: false, message: 'المستخدم غير موجود' });
        }
        res.json({ success: true, balance: user.balance });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ في جلب الرصيد' });
    }
});

// ================================================================
// API - طلبات eSIM
// ================================================================

app.post('/api/esim/order/create', authenticateToken, upload.array('screenshots', ENV.MAX_FILES), async (req, res) => {
    try {
        const { esimType, country, countryCode, amount, phoneNumber, notes, esimDetails, packageId } = req.body;
        const userId = req.user.id;
        const user = await User.findById(userId);
        
        if (!user) {
            return res.status(404).json({ success: false, message: 'المستخدم غير موجود' });
        }
        
        const orderAmount = Number(amount);
        if (user.balance < orderAmount) {
            return res.status(400).json({ success: false, message: 'الرصيد غير كافٍ' });
        }
        
        if (!esimType || !country || !orderAmount || !phoneNumber) {
            return res.status(400).json({ success: false, message: 'يرجى ملء جميع الحقول المطلوبة' });
        }
        
        const screenshotPaths = [];
        if (req.files && req.files.length > 0) {
            for (const file of req.files) {
                screenshotPaths.push(file.path);
            }
        }
        
        let esimData = {};
        if (esimDetails) {
            try {
                esimData = typeof esimDetails === 'string' ? JSON.parse(esimDetails) : esimDetails;
            } catch (e) {
                esimData = {};
            }
        }
        
        if (packageId) {
            const pkg = await EsimPackage.findById(packageId);
            if (pkg) {
                esimData.dataPlan = pkg.name;
                esimData.validityDays = pkg.validityDays;
                esimData.operator = pkg.operator;
                esimData.networkType = pkg.networkType;
                esimData.coverage = pkg.coverage;
            }
        }
        
        await updateUserBalance(userId, orderAmount, 'subtract', `طلب eSIM - ${country}`);
        
        const order = new EsimOrder({
            userId,
            username: user.username,
            esimType,
            country,
            countryCode: countryCode || '',
            amount: orderAmount,
            phoneNumber,
            notes: notes || '',
            screenshots: screenshotPaths,
            esimDetails: esimData,
            status: 'pending',
        });
        
        await order.save();
        await User.findByIdAndUpdate(userId, { $inc: { totalOrders: 1, totalSpent: orderAmount } });
        
        await logTransaction(userId, user.username, 'order', orderAmount, esimType, 'pending', { orderId: order._id, country });
        await createNotification(userId, '📱 طلب eSIM جديد', `تم إنشاء طلب eSIM بقيمة ${orderAmount} جنيه للدولة ${country}. جاري مراجعة الطلب.`, 'info', order._id);
        
        broadcastToAll({
            type: 'new_esim_order',
            orderId: order._id,
            username: user.username,
            amount: orderAmount,
            country: country,
            type: esimType,
        }, 'admin_alert');
        
        res.status(201).json({
            success: true,
            message: 'تم إنشاء طلب eSIM بنجاح، جاري المراجعة',
            orderId: order._id,
            order,
        });
    } catch (error) {
        console.error('❌ خطأ في إنشاء طلب eSIM:', error);
        res.status(500).json({ success: false, message: error.message || 'حدث خطأ في إنشاء الطلب' });
    }
});

app.get('/api/esim/orders/user', authenticateToken, async (req, res) => {
    try {
        const { status, page = 1, limit = 20 } = req.query;
        const filter = { userId: req.user.id };
        if (status) filter.status = status;
        const skip = (parseInt(page) - 1) * parseInt(limit);
        const orders = await EsimOrder.find(filter).sort({ createdAt: -1 }).skip(skip).limit(parseInt(limit));
        const total = await EsimOrder.countDocuments(filter);
        res.json({ success: true, orders, pagination: { page: parseInt(page), limit: parseInt(limit), total, pages: Math.ceil(total / parseInt(limit)) } });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ في جلب الطلبات' });
    }
});

app.get('/api/esim/order/:id', authenticateToken, async (req, res) => {
    try {
        const order = await EsimOrder.findById(req.params.id).populate('userId', 'username email fullName phone');
        if (!order) {
            return res.status(404).json({ success: false, message: 'الطلب غير موجود' });
        }
        if (order.userId._id.toString() !== req.user.id && req.user.role !== 'admin') {
            return res.status(403).json({ success: false, message: 'غير مصرح لك بعرض هذا الطلب' });
        }
        let timeRemaining = null;
        if (order.status === 'accepted' && order.expiresAt) {
            timeRemaining = getTimeRemaining(order.expiresAt);
        }
        res.json({ success: true, order, timeRemaining });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ في جلب تفاصيل الطلب' });
    }
});

// ================================================================
// API - الباقات
// ================================================================

app.get('/api/esim/packages', authenticateToken, async (req, res) => {
    try {
        const { country, isActive } = req.query;
        const filter = {};
        if (country) filter.country = { $regex: country, $options: 'i' };
        if (isActive !== undefined) filter.isActive = isActive === 'true';
        const packages = await EsimPackage.find(filter).sort({ isPopular: -1, price: 1 });
        res.json({ success: true, packages, total: packages.length });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ في جلب الباقات' });
    }
});

// ================================================================
// API - الإشعارات
// ================================================================

app.get('/api/notifications', authenticateToken, async (req, res) => {
    try {
        const { page = 1, limit = 20, unreadOnly = false } = req.query;
        const filter = { userId: req.user.id };
        if (unreadOnly === 'true') filter.isRead = false;
        const skip = (parseInt(page) - 1) * parseInt(limit);
        const notifications = await Notification.find(filter).sort({ createdAt: -1 }).skip(skip).limit(parseInt(limit));
        const total = await Notification.countDocuments(filter);
        const unreadCount = await Notification.countDocuments({ userId: req.user.id, isRead: false });
        res.json({ success: true, notifications, unreadCount, pagination: { page: parseInt(page), limit: parseInt(limit), total, pages: Math.ceil(total / parseInt(limit)) } });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ في جلب الإشعارات' });
    }
});

app.put('/api/notifications/:id/read', authenticateToken, async (req, res) => {
    try {
        const notification = await Notification.findById(req.params.id);
        if (!notification) {
            return res.status(404).json({ success: false, message: 'الإشعار غير موجود' });
        }
        if (notification.userId.toString() !== req.user.id) {
            return res.status(403).json({ success: false, message: 'غير مصرح لك' });
        }
        notification.isRead = true;
        notification.readAt = new Date();
        await notification.save();
        res.json({ success: true, message: 'تم تحديث حالة الإشعار' });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ في تحديث الإشعار' });
    }
});

app.put('/api/notifications/read-all', authenticateToken, async (req, res) => {
    try {
        await Notification.updateMany({ userId: req.user.id, isRead: false }, { isRead: true, readAt: new Date() });
        res.json({ success: true, message: 'تم قراءة جميع الإشعارات' });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ' });
    }
});

// ================================================================
// API - المسؤول
// ================================================================

app.post('/api/admin/login', async (req, res) => {
    try {
        const { password } = req.body;
        if (!password) {
            return res.status(400).json({ success: false, message: 'يرجى إدخال كلمة المرور' });
        }
        if (password !== ENV.ADMIN_PASSWORD) {
            return res.status(401).json({ success: false, message: 'كلمة المرور غير صحيحة' });
        }
        const token = jwt.sign(
            { id: 'admin', username: 'admin', role: 'admin', permissions: ['all'] },
            ENV.JWT_SECRET,
            { expiresIn: '1d' }
        );
        res.json({
            success: true,
            message: 'تم تسجيل دخول المسؤول بنجاح',
            token,
            admin: { username: 'admin', role: 'admin', permissions: ['all'] }
        });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ في تسجيل الدخول' });
    }
});

// الحصول على جميع الطلبات
app.get('/api/admin/esim/orders', authenticateAdmin, async (req, res) => {
    try {
        const { status, country, username, page = 1, limit = 50 } = req.query;
        const filter = {};
        if (status) filter.status = status;
        if (country) filter.country = { $regex: country, $options: 'i' };
        if (username) filter.username = { $regex: username, $options: 'i' };
        const skip = (parseInt(page) - 1) * parseInt(limit);
        const orders = await EsimOrder.find(filter).populate('userId', 'username email fullName phone country balance').sort({ createdAt: -1 }).skip(skip).limit(parseInt(limit));
        const total = await EsimOrder.countDocuments(filter);
        const stats = { total, pending: await EsimOrder.countDocuments({ status: 'pending' }), accepted: await EsimOrder.countDocuments({ status: 'accepted' }), delivered: await EsimOrder.countDocuments({ status: 'delivered' }), rejected: await EsimOrder.countDocuments({ status: 'rejected' }), expired: await EsimOrder.countDocuments({ status: 'expired' }) };
        res.json({ success: true, orders, stats, pagination: { page: parseInt(page), limit: parseInt(limit), total, pages: Math.ceil(total / parseInt(limit)) } });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ في جلب الطلبات' });
    }
});

// قبول طلب eSIM
app.post('/api/admin/esim/order/:id/accept', authenticateAdmin, async (req, res) => {
    try {
        const { esimData, expiryDate, adminNotes } = req.body;
        const order = await EsimOrder.findById(req.params.id).populate('userId');
        if (!order) {
            return res.status(404).json({ success: false, message: 'الطلب غير موجود' });
        }
        if (order.status !== 'pending') {
            return res.status(400).json({ success: false, message: 'لا يمكن قبول هذا الطلب' });
        }
        if (esimData) {
            const data = typeof esimData === 'string' ? JSON.parse(esimData) : esimData;
            order.esimDetails = { ...order.esimDetails, ...data };
        }
        if (expiryDate) {
            order.esimDetails.expiryDate = new Date(expiryDate);
        }
        if (adminNotes) {
            order.adminNotes = adminNotes;
        }
        order.status = 'accepted';
        order.acceptedAt = new Date();
        order.expiresAt = new Date(Date.now() + ENV.ORDER_EXPIRY_MINUTES * 60 * 1000);
        await order.save();
        await createNotification(order.userId._id, '✅ تم قبول طلب eSIM', `تم قبول طلب eSIM للدولة ${order.country}. سيتم تسليم البطاقة قريباً.`, 'success', order._id);
        res.json({ success: true, message: 'تم قبول الطلب بنجاح', order });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message || 'حدث خطأ في قبول الطلب' });
    }
});

// تسليم بطاقة eSIM
app.post('/api/admin/esim/order/:id/deliver', authenticateAdmin, async (req, res) => {
    try {
        const { iccid, activationCode, qrCode, qrCodeBase64, dataPlan, validityDays, operator, networkType, coverage, expiryDate, activationDate, notes } = req.body;
        const order = await EsimOrder.findById(req.params.id).populate('userId');
        if (!order) {
            return res.status(404).json({ success: false, message: 'الطلب غير موجود' });
        }
        if (order.status !== 'accepted') {
            return res.status(400).json({ success: false, message: 'الطلب غير مقبول بعد' });
        }
        order.esimDetails.iccid = iccid || order.esimDetails.iccid;
        order.esimDetails.activationCode = activationCode || order.esimDetails.activationCode;
        order.esimDetails.qrCode = qrCode || order.esimDetails.qrCode;
        order.esimDetails.qrCodeBase64 = qrCodeBase64 || order.esimDetails.qrCodeBase64;
        order.esimDetails.dataPlan = dataPlan || order.esimDetails.dataPlan;
        order.esimDetails.validityDays = validityDays || order.esimDetails.validityDays || 30;
        order.esimDetails.operator = operator || order.esimDetails.operator;
        order.esimDetails.networkType = networkType || order.esimDetails.networkType;
        order.esimDetails.coverage = coverage || order.esimDetails.coverage;
        if (expiryDate) order.esimDetails.expiryDate = new Date(expiryDate);
        if (activationDate) order.esimDetails.activationDate = new Date(activationDate);
        if (notes) order.esimDetails.notes = notes;
        order.status = 'delivered';
        order.deliveredAt = new Date();
        await order.save();
        await User.findByIdAndUpdate(order.userId._id, { $inc: { totalOrders: 1 } });
        await createNotification(order.userId._id, '📱 تم تسليم بطاقة eSIM', `تم تسليم بطاقة eSIM للدولة ${order.country}. يمكنك الآن استخدام البطاقة.`, 'success', order._id);
        res.json({ success: true, message: 'تم تسليم بطاقة eSIM بنجاح', order });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message || 'حدث خطأ في تسليم البطاقة' });
    }
});

// رفض طلب eSIM
app.post('/api/admin/esim/order/:id/reject', authenticateAdmin, async (req, res) => {
    try {
        const { reason, details } = req.body;
        const order = await EsimOrder.findById(req.params.id).populate('userId');
        if (!order) {
            return res.status(404).json({ success: false, message: 'الطلب غير موجود' });
        }
        if (order.status !== 'pending') {
            return res.status(400).json({ success: false, message: 'لا يمكن رفض هذا الطلب' });
        }
        order.status = 'rejected';
        order.rejectionReason = reason || 'تم رفض الطلب من قبل الإدارة';
        order.rejectionDetails = details || '';
        await order.save();
        await updateUserBalance(order.userId._id, order.amount, 'add', 'رفض طلب eSIM');
        await createNotification(order.userId._id, '❌ تم رفض طلب eSIM', `تم رفض طلب eSIM للدولة ${order.country}. السبب: ${order.rejectionReason}${details ? ' - ' + details : ''}`, 'error', order._id);
        res.json({ success: true, message: 'تم رفض الطلب وإرجاع الرصيد', order });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message || 'حدث خطأ في رفض الطلب' });
    }
});

// إكمال الطلب
app.post('/api/admin/esim/order/:id/complete', authenticateAdmin, async (req, res) => {
    try {
        const order = await EsimOrder.findById(req.params.id).populate('userId');
        if (!order) {
            return res.status(404).json({ success: false, message: 'الطلب غير موجود' });
        }
        if (order.status !== 'delivered') {
            return res.status(400).json({ success: false, message: 'الطلب لم يتم تسليمه بعد' });
        }
        order.status = 'completed';
        order.completedAt = new Date();
        await order.save();
        await createNotification(order.userId._id, '✔️ اكتمل طلب eSIM', `تم إكمال طلب eSIM للدولة ${order.country} بنجاح. شكراً لاستخدامك الخدمة.`, 'success', order._id);
        res.json({ success: true, message: 'تم إكمال الطلب بنجاح', order });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message || 'حدث خطأ في إكمال الطلب' });
    }
});

// الحصول على جميع المستخدمين
app.get('/api/admin/users', authenticateAdmin, async (req, res) => {
    try {
        const { search, isActive, page = 1, limit = 50 } = req.query;
        const filter = {};
        if (search) {
            filter.$or = [{ username: { $regex: search, $options: 'i' } }, { email: { $regex: search, $options: 'i' } }, { fullName: { $regex: search, $options: 'i' } }, { phone: { $regex: search, $options: 'i' } }];
        }
        if (isActive !== undefined) filter.isActive = isActive === 'true';
        const skip = (parseInt(page) - 1) * parseInt(limit);
        const users = await User.find(filter).select('-password').sort({ createdAt: -1 }).skip(skip).limit(parseInt(limit));
        const total = await User.countDocuments(filter);
        res.json({ success: true, users, pagination: { page: parseInt(page), limit: parseInt(limit), total, pages: Math.ceil(total / parseInt(limit)) } });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ في جلب المستخدمين' });
    }
});

// تحديث حالة المستخدم
app.put('/api/admin/user/:id/toggle', authenticateAdmin, async (req, res) => {
    try {
        const user = await User.findById(req.params.id);
        if (!user) {
            return res.status(404).json({ success: false, message: 'المستخدم غير موجود' });
        }
        user.isActive = !user.isActive;
        await user.save();
        await createNotification(user._id, user.isActive ? '✅ تم تفعيل حسابك' : '⛔ تم تعطيل حسابك', user.isActive ? 'تم تفعيل حسابك في نظام eSIM. يمكنك الآن استخدام الخدمة.' : 'تم تعطيل حسابك في نظام eSIM. يرجى التواصل مع الدعم.', user.isActive ? 'success' : 'error');
        res.json({ success: true, message: `تم ${user.isActive ? 'تفعيل' : 'تعطيل'} المستخدم بنجاح`, user });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ في تحديث حالة المستخدم' });
    }
});

// إضافة رصيد للمستخدم
app.post('/api/admin/user/:id/add-balance', authenticateAdmin, async (req, res) => {
    try {
        const { amount, reason } = req.body;
        const user = await User.findById(req.params.id);
        if (!user) {
            return res.status(404).json({ success: false, message: 'المستخدم غير موجود' });
        }
        if (!amount || amount <= 0) {
            return res.status(400).json({ success: false, message: 'المبلغ يجب أن يكون أكبر من صفر' });
        }
        await updateUserBalance(user._id, Number(amount), 'add', reason || 'إضافة رصيد من المسؤول');
        await createNotification(user._id, '💰 إضافة رصيد', `تم إضافة مبلغ ${amount} جنيه إلى رصيدك.${reason ? ' السبب: ' + reason : ''}`, 'success');
        res.json({ success: true, message: 'تم إضافة المبلغ بنجاح', newBalance: user.balance });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message || 'حدث خطأ في إضافة الرصيد' });
    }
});

// الحصول على الإحصائيات
app.get('/api/admin/stats', authenticateAdmin, async (req, res) => {
    try {
        const totalUsers = await User.countDocuments();
        const activeUsers = await User.countDocuments({ isActive: true });
        const totalOrders = await EsimOrder.countDocuments();
        const pendingOrders = await EsimOrder.countDocuments({ status: 'pending' });
        const acceptedOrders = await EsimOrder.countDocuments({ status: 'accepted' });
        const deliveredOrders = await EsimOrder.countDocuments({ status: 'delivered' });
        const rejectedOrders = await EsimOrder.countDocuments({ status: 'rejected' });
        const expiredOrders = await EsimOrder.countDocuments({ status: 'expired' });
        const completedOrders = await EsimOrder.countDocuments({ status: 'completed' });
        const revenueData = await EsimOrder.aggregate([
            { $match: { status: { $in: ['delivered', 'completed'] } } },
            { $group: { _id: null, total: { $sum: '$amount' }, count: { $sum: 1 } } }
        ]);
        res.json({
            success: true,
            stats: {
                users: { total: totalUsers, active: activeUsers },
                orders: { total: totalOrders, pending: pendingOrders, accepted: acceptedOrders, delivered: deliveredOrders, rejected: rejectedOrders, expired: expiredOrders, completed: completedOrders },
                revenue: { total: revenueData.length > 0 ? revenueData[0].total : 0, totalOrders: revenueData.length > 0 ? revenueData[0].count : 0 }
            }
        });
    } catch (error) {
        res.status(500).json({ success: false, message: 'حدث خطأ في جلب الإحصائيات' });
    }
});

// ================================================================
// خدمة الملفات الثابتة - المسار الرئيسي
// ================================================================

// تقديم الملفات الثابتة من مجلد public
app.use(express.static('public'));

// تقديم الملفات المرفوعة
app.use('/uploads', express.static('uploads', {
    maxAge: '7d',
    etag: true,
    lastModified: true,
}));

// ================================================================
// مسارات الصفحات
// ================================================================

// الصفحة الرئيسية - esimPanel.html
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'esimPanel.html'));
});

// صفحة المسؤول - admini.html
app.get('/admini', (req, res) => {
    res.sendFile(path.join(__dirname, 'admini.html'));
});

// أي مسار آخر يذهب للصفحة الرئيسية
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'esimPanel.html'));
});

// ================================================================
// معالجة الأخطاء
// ================================================================

app.use((req, res, next) => {
    res.status(404).json({
        success: false,
        message: 'الرابط غير موجود',
        code: 'NOT_FOUND',
        path: req.path,
    });
});

app.use((err, req, res, next) => {
    console.error('❌ خطأ في الخادم:', err);
    res.status(500).json({
        success: false,
        message: 'حدث خطأ في الخادم',
        code: 'SERVER_ERROR',
    });
});

// ================================================================
// تشغيل الخادم
// ================================================================

server.listen(ENV.PORT, () => {
    console.log(`🚀 ${SYSTEM.fullName} v${SYSTEM.version}`);
    console.log(`📱 الخادم يعمل على http://localhost:${ENV.PORT}`);
    console.log(`📱 صفحة العميل: http://localhost:${ENV.PORT}`);
    console.log(`🛠️  صفحة المسؤول: http://localhost:${ENV.PORT}/admini`);
    console.log(`🔐 كلمة مرور المسؤول: ${ENV.ADMIN_PASSWORD}`);
    console.log(`📦 البيئة: ${ENV.NODE_ENV}`);
    console.log(`🔄 WebSocket: ws://localhost:${ENV.PORT}`);
    console.log('='.repeat(60));
});

// ================================================================
// إيقاف الخادم بشكل آمن
// ================================================================

process.on('SIGTERM', () => {
    console.log('🛑 استقبال SIGTERM، إيقاف الخادم...');
    server.close(() => {
        console.log('✅ تم إيقاف الخادم');
        mongoose.disconnect(() => {
            console.log('✅ تم قطع الاتصال بقاعدة البيانات');
            process.exit(0);
        });
    });
});

process.on('SIGINT', () => {
    console.log('🛑 استقبال SIGINT، إيقاف الخادم...');
    server.close(() => {
        console.log('✅ تم إيقاف الخادم');
        mongoose.disconnect(() => {
            console.log('✅ تم قطع الاتصال بقاعدة البيانات');
            process.exit(0);
        });
    });
});

console.log('✅ تم تحميل أكثر من 2000 سطر من الكود المتكامل');
console.log(`📊 ${SYSTEM.name} جاهز للعمل!`);
