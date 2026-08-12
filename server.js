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
const cluster = require('cluster');

// ================================================================
// معلومات النظام - eSIM
// ================================================================
const SYSTEM = {
    name: 'eSIM',
    fullName: 'eSIM Management System',
    version: '3.0.0',
    build: '2026.08.12',
    copyright: '© 2026 eSIM - جميع الحقوق محفوظة',
    description: 'نظام متكامل لإدارة بطاقات eSIM الرقمية',
    website: 'https://esim.example.com',
    supportEmail: 'support@esim.example.com',
    adminEmail: 'admin@esim.example.com',
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
    MONGO_URI: process.env.MONGO_URI || 'mongodb://localhost:27017/esim_system',
    MAX_FILE_SIZE: parseInt(process.env.MAX_FILE_SIZE) || 5 * 1024 * 1024,
    MAX_FILES: parseInt(process.env.MAX_FILES) || 5,
    ORDER_EXPIRY_MINUTES: parseInt(process.env.ORDER_EXPIRY_MINUTES) || 3,
    SESSION_SECRET: process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex'),
    RATE_LIMIT_WINDOW: parseInt(process.env.RATE_LIMIT_WINDOW) || 60000,
    RATE_LIMIT_MAX: parseInt(process.env.RATE_LIMIT_MAX) || 100,
};

// ================================================================
// إعدادات الخادم المتقدم
// ================================================================
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ 
    server,
    perMessageDeflate: true,
    clientTracking: true,
});

// ================================================================
// Middleware متقدم
// ================================================================

// CORS متقدم
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
    credentials: true,
    preflightContinue: false,
    optionsSuccessStatus: 204,
}));

// Body Parser مع حدود كبيرة
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.static('public', {
    maxAge: '1d',
    etag: true,
    lastModified: true,
}));

// ================================================================
// تكوين Multer المتقدم
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
// الاتصال بقاعدة البيانات مع إعادة محاولة متقدمة
// ================================================================
let isDbConnected = false;

const connectDB = async (retries = 5, delay = 5000) => {
    for (let i = 0; i < retries; i++) {
        try {
            await mongoose.connect(ENV.MONGO_URI, {
                useNewUrlParser: true,
                useUnifiedTopology: true,
                serverSelectionTimeoutMS: 5000,
                socketTimeoutMS: 45000,
                family: 4,
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
    process.exit(1);
};

connectDB();

// ================================================================
// مراقبة حالة الاتصال بقاعدة البيانات
// ================================================================
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
// نماذج قاعدة البيانات المتقدمة
// ================================================================

// نموذج المستخدم المتقدم
const UserSchema = new mongoose.Schema({
    username: { 
        type: String, 
        required: [true, 'اسم المستخدم مطلوب'],
        unique: true,
        trim: true,
        minlength: [3, 'اسم المستخدم يجب أن يكون 3 أحرف على الأقل'],
        maxlength: [30, 'اسم المستخدم يجب أن يكون 30 حرفاً كحد أقصى'],
        match: [/^[a-zA-Z0-9_]+$/, 'اسم المستخدم يحتوي على أحرف غير مسموحة'],
    },
    email: { 
        type: String, 
        required: [true, 'البريد الإلكتروني مطلوب'],
        unique: true,
        trim: true,
        lowercase: true,
        match: [/^[^\s@]+@[^\s@]+\.[^\s@]+$/, 'البريد الإلكتروني غير صحيح'],
    },
    password: { 
        type: String, 
        required: [true, 'كلمة المرور مطلوبة'],
        minlength: [6, 'كلمة المرور يجب أن تكون 6 أحرف على الأقل'],
    },
    fullName: { 
        type: String, 
        default: '',
        trim: true,
        maxlength: [50, 'الاسم الكامل يجب أن يكون 50 حرفاً كحد أقصى'],
    },
    phone: { 
        type: String, 
        default: '',
        trim: true,
        match: [/^[\+]?[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}$/, 'رقم الهاتف غير صحيح'],
    },
    country: { 
        type: String, 
        default: '',
        trim: true,
        maxlength: [50, 'اسم الدولة يجب أن يكون 50 حرفاً كحد أقصى'],
    },
    countryCode: { 
        type: String, 
        default: '',
        uppercase: true,
        match: [/^[A-Z]{2}$/, 'رمز الدولة يجب أن يكون حرفين كبيرين'],
    },
    balance: { 
        type: Number, 
        default: 0,
        min: [0, 'الرصيد لا يمكن أن يكون سالباً'],
        get: v => Number(v.toFixed(2)),
        set: v => Number(v.toFixed(2)),
    },
    totalOrders: { type: Number, default: 0 },
    totalSpent: { type: Number, default: 0 },
    totalRefunds: { type: Number, default: 0 },
    isActive: { type: Boolean, default: true },
    isVerified: { type: Boolean, default: false },
    verificationToken: { type: String, default: '' },
    resetPasswordToken: { type: String, default: '' },
    resetPasswordExpires: { type: Date },
    lastLogin: { type: Date },
    lastLoginIP: { type: String, default: '' },
    lastLoginDevice: { type: String, default: '' },
    ipAddress: { type: String, default: '' },
    deviceInfo: { type: String, default: '' },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
    preferences: {
        language: { type: String, default: 'ar', enum: ['ar', 'en'] },
        theme: { type: String, default: 'dark', enum: ['dark', 'light'] },
        notifications: { type: Boolean, default: true },
    },
    referredBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
    referralCode: { type: String, unique: true, sparse: true },
    referralCount: { type: Number, default: 0 },
    referralEarnings: { type: Number, default: 0 },
});

// نموذج طلبات eSIM المتقدم
const EsimOrderSchema = new mongoose.Schema({
    userId: { 
        type: mongoose.Schema.Types.ObjectId, 
        ref: 'User', 
        required: [true, 'معرف المستخدم مطلوب'],
        index: true,
    },
    username: { 
        type: String, 
        required: true,
        index: true,
    },
    
    // معلومات البطاقة
    esimType: { 
        type: String, 
        enum: ['vodafone', 'instapay', 'cash', 'data_only', 'voice_data', 'unlimited'], 
        required: true,
    },
    country: { 
        type: String, 
        required: [true, 'اسم الدولة مطلوب'],
        index: true,
    },
    countryCode: { 
        type: String, 
        uppercase: true,
        match: [/^[A-Z]{2}$/, 'رمز الدولة يجب أن يكون حرفين كبيرين'],
    },
    amount: { 
        type: Number, 
        required: true,
        min: [1, 'المبلغ يجب أن يكون أكبر من صفر'],
        get: v => Number(v.toFixed(2)),
        set: v => Number(v.toFixed(2)),
    },
    phoneNumber: { 
        type: String, 
        required: [true, 'رقم الهاتف مطلوب'],
        trim: true,
    },
    
    // تفاصيل البطاقة
    esimDetails: {
        iccid: { 
            type: String, 
            default: '',
            match: [/^[0-9]{19,20}$/, 'ICCID يجب أن يكون 19-20 رقم'],
        },
        activationCode: { type: String, default: '' },
        qrCode: { type: String, default: '' },
        qrCodeBase64: { type: String, default: '' },
        expiryDate: { type: Date },
        dataPlan: { type: String, default: '' },
        validityDays: { type: Number, default: 30, min: 1, max: 365 },
        operator: { type: String, default: '' },
        networkType: { type: String, default: '5G', enum: ['4G', '5G', 'LTE'] },
        coverage: { type: String, default: '' },
        activationDate: { type: Date },
        notes: { type: String, default: '' },
    },
    
    // حالة الطلب
    status: { 
        type: String, 
        enum: ['pending', 'accepted', 'rejected', 'expired', 'delivered', 'completed', 'refunded'], 
        default: 'pending',
        index: true,
    },
    
    // التواريخ
    acceptedAt: { type: Date },
    expiresAt: { type: Date },
    deliveredAt: { type: Date },
    completedAt: { type: Date },
    refundedAt: { type: Date },
    rejectionReason: { type: String, default: '' },
    rejectionDetails: { type: String, default: '' },
    
    // المرفقات
    screenshots: [{ 
        type: String,
        validate: {
            validator: function(v) {
                return v.length <= 5;
            },
            message: 'الحد الأقصى للمرفقات هو 5 ملفات'
        }
    }],
    notes: { type: String, default: '', maxlength: 500 },
    adminNotes: { type: String, default: '', maxlength: 500 },
    
    // وقت إتمام الطلب
    timeRemaining: { type: Number, default: 180 },
    createdAt: { type: Date, default: Date.now, index: true },
    updatedAt: { type: Date, default: Date.now },
    
    // إحصائيات إضافية
    reviewCount: { type: Number, default: 0 },
    rating: { type: Number, default: 0, min: 0, max: 5 },
    feedback: { type: String, default: '' },
});

// نموذج الإشعارات المتقدم
const NotificationSchema = new mongoose.Schema({
    userId: { 
        type: mongoose.Schema.Types.ObjectId, 
        ref: 'User', 
        required: true,
        index: true,
    },
    title: { 
        type: String, 
        required: [true, 'عنوان الإشعار مطلوب'],
        maxlength: [100, 'العنوان يجب أن يكون 100 حرف كحد أقصى'],
    },
    message: { 
        type: String, 
        required: [true, 'نص الإشعار مطلوب'],
        maxlength: [500, 'النص يجب أن يكون 500 حرف كحد أقصى'],
    },
    type: { 
        type: String, 
        enum: ['info', 'success', 'warning', 'error', 'order', 'delivery'], 
        default: 'info' 
    },
    isRead: { type: Boolean, default: false },
    isImportant: { type: Boolean, default: false },
    relatedOrderId: { 
        type: mongoose.Schema.Types.ObjectId, 
        ref: 'EsimOrder',
        index: true,
    },
    metadata: { type: mongoose.Schema.Types.Mixed, default: {} },
    createdAt: { type: Date, default: Date.now, index: true },
    readAt: { type: Date },
});

// نموذج سجل العمليات المتقدم
const TransactionLogSchema = new mongoose.Schema({
    userId: { 
        type: mongoose.Schema.Types.ObjectId, 
        ref: 'User',
        index: true,
    },
    username: { type: String, index: true },
    type: { 
        type: String, 
        enum: ['order', 'delivery', 'refund', 'adjustment', 'deposit', 'withdrawal', 'referral'], 
        required: true 
    },
    amount: { 
        type: Number,
        get: v => Number(v.toFixed(2)),
        set: v => Number(v.toFixed(2)),
    },
    method: { type: String, default: '' },
    status: { type: String, default: 'pending' },
    details: { type: mongoose.Schema.Types.Mixed, default: {} },
    ipAddress: { type: String, default: '' },
    userAgent: { type: String, default: '' },
    createdAt: { type: Date, default: Date.now, index: true },
});

// نموذج باقات eSIM
const EsimPackageSchema = new mongoose.Schema({
    name: { 
        type: String, 
        required: [true, 'اسم الباقة مطلوب'],
        unique: true,
        trim: true,
        maxlength: [100, 'الاسم يجب أن يكون 100 حرف كحد أقصى'],
    },
    country: { 
        type: String, 
        required: [true, 'اسم الدولة مطلوب'],
        index: true,
    },
    countryCode: { 
        type: String, 
        uppercase: true,
        match: [/^[A-Z]{2}$/, 'رمز الدولة يجب أن يكون حرفين كبيرين'],
    },
    dataAmount: { 
        type: String, 
        required: [true, 'كمية البيانات مطلوبة'],
        enum: ['1GB', '2GB', '3GB', '5GB', '10GB', '20GB', '50GB', '100GB', 'Unlimited'],
    },
    validityDays: { 
        type: Number, 
        required: true,
        min: [1, 'صلاحية الباقة يجب أن تكون يوم واحد على الأقل'],
        max: [365, 'صلاحية الباقة لا تتجاوز 365 يوم'],
    },
    price: { 
        type: Number, 
        required: true,
        min: [0, 'السعر لا يمكن أن يكون سالباً'],
        get: v => Number(v.toFixed(2)),
        set: v => Number(v.toFixed(2)),
    },
    discountedPrice: { 
        type: Number,
        get: v => Number(v.toFixed(2)),
        set: v => Number(v.toFixed(2)),
    },
    currency: { type: String, default: 'EGP' },
    isActive: { type: Boolean, default: true },
    isPopular: { type: Boolean, default: false },
    isFeatured: { type: Boolean, default: false },
    description: { type: String, default: '', maxlength: 500 },
    features: [{ type: String }],
    operator: { type: String, default: '' },
    networkType: { type: String, default: '5G' },
    coverage: { type: String, default: '' },
    icon: { type: String, default: '📱' },
    color: { type: String, default: '#6C3CE1' },
    orderCount: { type: Number, default: 0 },
    rating: { type: Number, default: 0, min: 0, max: 5 },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
});

// نموذج إعدادات النظام
const SettingsSchema = new mongoose.Schema({
    key: { type: String, unique: true, required: true },
    value: { type: mongoose.Schema.Types.Mixed },
    description: { type: String, default: '' },
    type: { 
        type: String, 
        enum: ['string', 'number', 'boolean', 'array', 'object'], 
        default: 'string' 
    },
    isPublic: { type: Boolean, default: false },
    updatedAt: { type: Date, default: Date.now },
    updatedBy: { type: String, default: '' },
});

// نموذج الإحالات
const ReferralSchema = new mongoose.Schema({
    referrerId: { 
        type: mongoose.Schema.Types.ObjectId, 
        ref: 'User', 
        required: true 
    },
    referredId: { 
        type: mongoose.Schema.Types.ObjectId, 
        ref: 'User', 
        required: true 
    },
    code: { type: String, required: true },
    status: { 
        type: String, 
        enum: ['pending', 'converted', 'rewarded'], 
        default: 'pending' 
    },
    rewardAmount: { type: Number, default: 0 },
    createdAt: { type: Date, default: Date.now },
    convertedAt: { type: Date },
});

// ================================================================
// إنشاء النماذج مع الفهارس المتقدمة
// ================================================================
const User = mongoose.model('User', UserSchema);
const EsimOrder = mongoose.model('EsimOrder', EsimOrderSchema);
const Notification = mongoose.model('Notification', NotificationSchema);
const TransactionLog = mongoose.model('TransactionLog', TransactionLogSchema);
const EsimPackage = mongoose.model('EsimPackage', EsimPackageSchema);
const Settings = mongoose.model('Settings', SettingsSchema);
const Referral = mongoose.model('Referral', ReferralSchema);

// إنشاء الفهارس
UserSchema.index({ username: 1, email: 1 });
UserSchema.index({ referralCode: 1 }, { unique: true, sparse: true });
EsimOrderSchema.index({ userId: 1, createdAt: -1 });
EsimOrderSchema.index({ status: 1, createdAt: -1 });
EsimOrderSchema.index({ country: 1, status: 1 });
NotificationSchema.index({ userId: 1, createdAt: -1 });
TransactionLogSchema.index({ userId: 1, createdAt: -1 });
EsimPackageSchema.index({ country: 1, isActive: 1 });

// ================================================================
// دوال مساعدة متقدمة
// ================================================================

// توليد كود إحالة فريد
function generateReferralCode() {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let code = '';
    for (let i = 0; i < 8; i++) {
        code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
}

// التحقق من صحة البيانات
function validateEmail(email) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function validatePhone(phone) {
    return /^[\+]?[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}$/.test(phone);
}

function validateUsername(username) {
    return /^[a-zA-Z0-9_]{3,30}$/.test(username);
}

function validatePassword(password) {
    return password.length >= 6;
}

// حساب وقت متبقي
function getTimeRemaining(expiresAt) {
    const now = Date.now();
    const expiry = new Date(expiresAt).getTime();
    const remaining = Math.max(0, Math.floor((expiry - now) / 1000));
    return {
        seconds: remaining,
        minutes: Math.floor(remaining / 60),
        hours: Math.floor(remaining / 3600),
        days: Math.floor(remaining / 86400),
        isExpired: remaining <= 0,
    };
}

// إنشاء إشعار
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
        
        // إرسال عبر WebSocket
        broadcastNotification(userId, notification);
        
        return notification;
    } catch (error) {
        console.error('❌ خطأ في إنشاء الإشعار:', error);
        return null;
    }
}

// بث الإشعار عبر WebSocket
function broadcastNotification(userId, notification) {
    if (!wss || !wss.clients) return;
    
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN && client.userId === userId) {
            try {
                client.send(JSON.stringify({
                    type: 'notification',
                    data: {
                        _id: notification._id,
                        title: notification.title,
                        message: notification.message,
                        type: notification.type,
                        createdAt: notification.createdAt,
                        metadata: notification.metadata,
                    }
                }));
            } catch (error) {
                console.error('❌ خطأ في إرسال الإشعار عبر WebSocket:', error);
            }
        }
    });
}

// تسجيل عملية
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
            ipAddress: details.ipAddress || '',
            userAgent: details.userAgent || '',
        });
        await log.save();
        return log;
    } catch (error) {
        console.error('❌ خطأ في تسجيل العملية:', error);
        return null;
    }
}

// تحديث رصيد المستخدم
async function updateUserBalance(userId, amount, operation = 'add', reason = '') {
    try {
        const user = await User.findById(userId);
        if (!user) {
            throw new Error('المستخدم غير موجود');
        }
        
        if (operation === 'add') {
            user.balance = Number((user.balance + amount).toFixed(2));
        } else if (operation === 'subtract') {
            if (user.balance < amount) {
                throw new Error('الرصيد غير كافٍ');
            }
            user.balance = Number((user.balance - amount).toFixed(2));
        } else {
            throw new Error('عملية غير صالحة');
        }
        
        await user.save();
        
        // تسجيل العملية
        await logTransaction(
            userId,
            user.username,
            operation === 'add' ? 'deposit' : 'withdrawal',
            amount,
            'system',
            'completed',
            { reason, newBalance: user.balance }
        );
        
        return user;
    } catch (error) {
        console.error('❌ خطأ في تحديث الرصيد:', error);
        throw error;
    }
}

// التحقق من صلاحية التوكن
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
                    message: 'توكن غير صالح أو منتهي الصلاحية',
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

// ================================================================
// WebSocket المتقدم
// ================================================================
const connectedClients = new Map();

wss.on('connection', (ws, req) => {
    const clientId = uuidv4();
    connectedClients.set(clientId, { ws, userId: null, isAdmin: false });
    
    console.log(`🟢 اتصال WebSocket جديد: ${clientId}`);
    console.log(`📊 عدد المتصلين: ${connectedClients.size}`);
    
    // إرسال رسالة ترحيب
    ws.send(JSON.stringify({
        type: 'welcome',
        data: {
            message: 'مرحباً بك في نظام eSIM',
            clientId,
            timestamp: new Date().toISOString(),
        }
    }));
    
    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);
            const client = connectedClients.get(clientId);
            
            if (data.type === 'auth') {
                if (data.userId) {
                    client.userId = data.userId;
                    ws.userId = data.userId;
                    
                    // إرسال الإشعارات غير المقروءة
                    const notifications = await Notification.find({ 
                        userId: data.userId, 
                        isRead: false 
                    }).sort({ createdAt: -1 }).limit(20);
                    
                    if (notifications.length > 0) {
                        ws.send(JSON.stringify({
                            type: 'unread_notifications',
                            data: notifications
                        }));
                    }
                    
                    console.log(`🔗 مستخدم مصادق: ${data.userId}`);
                }
                
                if (data.isAdmin) {
                    client.isAdmin = true;
                    ws.isAdmin = true;
                    console.log('🔗 مسؤول مصادق');
                }
            }
            
            if (data.type === 'ping') {
                ws.send(JSON.stringify({
                    type: 'pong',
                    timestamp: new Date().toISOString()
                }));
            }
            
            if (data.type === 'read_notification' && data.notificationId) {
                await Notification.findByIdAndUpdate(data.notificationId, {
                    isRead: true,
                    readAt: new Date()
                });
            }
        } catch (error) {
            console.error('❌ خطأ في معالجة رسالة WebSocket:', error);
        }
    });
    
    ws.on('close', () => {
        connectedClients.delete(clientId);
        console.log(`🔴 تم قطع اتصال WebSocket: ${clientId}`);
        console.log(`📊 عدد المتصلين: ${connectedClients.size}`);
    });
    
    ws.on('error', (error) => {
        console.error(`❌ خطأ في WebSocket ${clientId}:`, error);
    });
});

// ================================================================
// بث رسالة لجميع العملاء
// ================================================================
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
// المهام المجدولة المتقدمة
// ================================================================

// 1. انتهاء صلاحية الطلبات (كل دقيقة)
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
            
            // إرجاع الرصيد
            await updateUserBalance(order.userId, order.amount, 'add', 'انتهاء صلاحية الطلب');
            
            // إشعار للمستخدم
            await createNotification(
                order.userId,
                '⏰ انتهت صلاحية طلب eSIM',
                `طلب eSIM رقم ${order._id} انتهت صلاحيته. تم إرجاع المبلغ إلى رصيدك.`,
                'warning',
                order._id
            );
            
            // تسجيل العملية
            await logTransaction(
                order.userId,
                order.username,
                'refund',
                order.amount,
                order.esimType,
                'expired',
                { orderId: order._id, reason: 'انتهت صلاحية الطلب' }
            );
        }
        
        if (expiredOrders.length > 0) {
            console.log(`⏰ تم انتهاء صلاحية ${expiredOrders.length} طلب eSIM وإرجاع المبالغ`);
        }
    } catch (error) {
        console.error('❌ خطأ في مهمة انتهاء الصلاحية:', error);
    }
});

// 2. تنظيف قاعدة البيانات (كل يوم)
cron.schedule('0 0 * * *', async () => {
    try {
        // حذف الإشعارات القديمة (أكثر من 30 يوم)
        const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
        const oldNotifications = await Notification.deleteMany({
            createdAt: { $lt: thirtyDaysAgo },
            isRead: true
        });
        
        console.log(`🧹 تم حذف ${oldNotifications.deletedCount} إشعار قديم`);
        
        // حذف السجلات القديمة (أكثر من 90 يوم)
        const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000);
        const oldLogs = await TransactionLog.deleteMany({
            createdAt: { $lt: ninetyDaysAgo }
        });
        
        console.log(`🧹 تم حذف ${oldLogs.deletedCount} سجل قديم`);
    } catch (error) {
        console.error('❌ خطأ في مهمة التنظيف:', error);
    }
});

// 3. تحديث الإحصائيات (كل ساعة)
cron.schedule('0 * * * *', async () => {
    try {
        // تحديث ترتيب الباقات
        const packages = await EsimPackage.find({ isActive: true });
        for (const pkg of packages) {
            const orderCount = await EsimOrder.countDocuments({
                'esimDetails.dataPlan': pkg.name,
                status: 'delivered'
            });
            await EsimPackage.findByIdAndUpdate(pkg._id, { orderCount });
        }
        console.log('📊 تم تحديث إحصائيات الباقات');
    } catch (error) {
        console.error('❌ خطأ في تحديث الإحصائيات:', error);
    }
});

// 4. إشعارات تذكيرية (كل ساعة)
cron.schedule('0 * * * *', async () => {
    try {
        const pendingOrders = await EsimOrder.find({
            status: 'pending',
            createdAt: { $lt: new Date(Date.now() - 60 * 60 * 1000) }
        });
        
        for (const order of pendingOrders) {
            // إشعار للمسؤولين
            broadcastToAll({
                type: 'pending_order_reminder',
                orderId: order._id,
                username: order.username,
                amount: order.amount,
                country: order.country,
            }, 'admin_alert');
        }
    } catch (error) {
        console.error('❌ خطأ في مهمة التذكير:', error);
    }
});

// ================================================================
// API - المصادقة المتقدمة
// ================================================================

// تسجيل مستخدم جديد
app.post('/api/register', async (req, res) => {
    try {
        const { username, email, password, fullName, phone, country, countryCode, referralCode } = req.body;
        
        // التحقق من صحة البيانات
        if (!username || !email || !password) {
            return res.status(400).json({
                success: false,
                message: 'يرجى ملء جميع الحقول المطلوبة',
                code: 'MISSING_FIELDS'
            });
        }
        
        if (!validateUsername(username)) {
            return res.status(400).json({
                success: false,
                message: 'اسم المستخدم غير صحيح (يجب أن يكون 3-30 حرفاً، أحرف وأرقام فقط)',
                code: 'INVALID_USERNAME'
            });
        }
        
        if (!validateEmail(email)) {
            return res.status(400).json({
                success: false,
                message: 'البريد الإلكتروني غير صحيح',
                code: 'INVALID_EMAIL'
            });
        }
        
        if (!validatePassword(password)) {
            return res.status(400).json({
                success: false,
                message: 'كلمة المرور يجب أن تكون 6 أحرف على الأقل',
                code: 'INVALID_PASSWORD'
            });
        }
        
        // التحقق من وجود المستخدم
        const existingUser = await User.findOne({ 
            $or: [{ username }, { email }] 
        });
        
        if (existingUser) {
            return res.status(400).json({
                success: false,
                message: 'اسم المستخدم أو البريد الإلكتروني مستخدم بالفعل',
                code: 'USER_EXISTS'
            });
        }
        
        // تشفير كلمة المرور
        const salt = await bcrypt.genSalt(12);
        const hashedPassword = await bcrypt.hash(password, salt);
        
        // إنشاء كود إحالة
        let refCode = generateReferralCode();
        let refExists = await User.findOne({ referralCode: refCode });
        while (refExists) {
            refCode = generateReferralCode();
            refExists = await User.findOne({ referralCode: refCode });
        }
        
        // إنشاء المستخدم
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
            preferences: {
                language: 'ar',
                theme: 'dark',
                notifications: true,
            }
        });
        
        await user.save();
        
        // معالجة الإحالة
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
                
                // تحديث إحصائيات المحيل
                await User.findByIdAndUpdate(referrer._id, {
                    $inc: { referralCount: 1 }
                });
            }
        }
        
        // إنشاء توكن
        const token = jwt.sign(
            { 
                id: user._id, 
                username: user.username, 
                email: user.email,
                role: 'user' 
            },
            ENV.JWT_SECRET,
            { expiresIn: ENV.JWT_EXPIRY }
        );
        
        // ترحيب
        await createNotification(
            user._id,
            '🎉 مرحباً بك في eSIM',
            'تم إنشاء حسابك بنجاح. يمكنك الآن طلب بطاقات eSIM بكل سهولة.',
            'success',
            null,
            { welcome: true }
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
                preferences: user.preferences,
            }
        });
    } catch (error) {
        console.error('❌ خطأ في التسجيل:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في إنشاء الحساب',
            code: 'REGISTER_ERROR'
        });
    }
});

// تسجيل الدخول
app.post('/api/login', async (req, res) => {
    try {
        const { username, password } = req.body;
        
        if (!username || !password) {
            return res.status(400).json({
                success: false,
                message: 'يرجى إدخال اسم المستخدم وكلمة المرور',
                code: 'MISSING_CREDENTIALS'
            });
        }
        
        // البحث عن المستخدم
        const user = await User.findOne({ 
            $or: [{ username }, { email: username }] 
        });
        
        if (!user) {
            return res.status(401).json({
                success: false,
                message: 'اسم المستخدم أو كلمة المرور غير صحيحة',
                code: 'INVALID_CREDENTIALS'
            });
        }
        
        // التحقق من كلمة المرور
        const validPassword = await bcrypt.compare(password, user.password);
        if (!validPassword) {
            return res.status(401).json({
                success: false,
                message: 'اسم المستخدم أو كلمة المرور غير صحيحة',
                code: 'INVALID_CREDENTIALS'
            });
        }
        
        // التحقق من حالة الحساب
        if (!user.isActive) {
            return res.status(403).json({
                success: false,
                message: 'الحساب معطل، يرجى التواصل مع الدعم',
                code: 'ACCOUNT_DISABLED'
            });
        }
        
        // تحديث آخر تسجيل دخول
        user.lastLogin = new Date();
        user.lastLoginIP = req.ip || req.connection.remoteAddress;
        user.lastLoginDevice = req.headers['user-agent'] || '';
        await user.save();
        
        // إنشاء توكن
        const token = jwt.sign(
            { 
                id: user._id, 
                username: user.username, 
                email: user.email,
                role: 'user' 
            },
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
                preferences: user.preferences,
                isActive: user.isActive,
                isVerified: user.isVerified,
                lastLogin: user.lastLogin,
            }
        });
    } catch (error) {
        console.error('❌ خطأ في تسجيل الدخول:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في تسجيل الدخول',
            code: 'LOGIN_ERROR'
        });
    }
});

// ================================================================
// API - العميل (Client Routes) المتقدمة
// ================================================================

// الحصول على ملف تعريف المستخدم
app.get('/api/user/profile', authenticateToken, async (req, res) => {
    try {
        const user = await User.findById(req.user.id)
            .select('-password')
            .populate('referredBy', 'username fullName');
        
        if (!user) {
            return res.status(404).json({
                success: false,
                message: 'المستخدم غير موجود',
                code: 'USER_NOT_FOUND'
            });
        }
        
        // جلب إحصائيات إضافية
        const orderCount = await EsimOrder.countDocuments({ 
            userId: user._id 
        });
        
        const deliveredOrders = await EsimOrder.countDocuments({ 
            userId: user._id, 
            status: 'delivered' 
        });
        
        const pendingOrders = await EsimOrder.countDocuments({ 
            userId: user._id, 
            status: 'pending' 
        });
        
        res.json({
            success: true,
            user,
            stats: {
                orderCount,
                deliveredOrders,
                pendingOrders,
            }
        });
    } catch (error) {
        console.error('❌ خطأ في جلب الملف الشخصي:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في جلب الملف الشخصي',
            code: 'PROFILE_ERROR'
        });
    }
});

// تحديث الملف الشخصي
app.put('/api/user/profile', authenticateToken, async (req, res) => {
    try {
        const { fullName, phone, country, countryCode, preferences } = req.body;
        
        const user = await User.findById(req.user.id);
        if (!user) {
            return res.status(404).json({
                success: false,
                message: 'المستخدم غير موجود',
                code: 'USER_NOT_FOUND'
            });
        }
        
        // تحديث الحقول
        if (fullName) user.fullName = fullName;
        if (phone) user.phone = phone;
        if (country) user.country = country;
        if (countryCode) user.countryCode = countryCode;
        if (preferences) {
            user.preferences = { ...user.preferences, ...preferences };
        }
        
        await user.save();
        
        res.json({
            success: true,
            message: 'تم تحديث الملف الشخصي بنجاح',
            user: {
                id: user._id,
                username: user.username,
                email: user.email,
                fullName: user.fullName,
                phone: user.phone,
                country: user.country,
                countryCode: user.countryCode,
                preferences: user.preferences,
            }
        });
    } catch (error) {
        console.error('❌ خطأ في تحديث الملف الشخصي:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في تحديث الملف الشخصي',
            code: 'UPDATE_ERROR'
        });
    }
});

// الحصول على الرصيد
app.get('/api/user/balance', authenticateToken, async (req, res) => {
    try {
        const user = await User.findById(req.user.id);
        if (!user) {
            return res.status(404).json({
                success: false,
                message: 'المستخدم غير موجود',
                code: 'USER_NOT_FOUND'
            });
        }
        
        // جلب آخر 5 عمليات
        const recentTransactions = await TransactionLog.find({ userId: user._id })
            .sort({ createdAt: -1 })
            .limit(5);
        
        res.json({
            success: true,
            balance: user.balance,
            totalSpent: user.totalSpent,
            totalOrders: user.totalOrders,
            recentTransactions,
        });
    } catch (error) {
        console.error('❌ خطأ في جلب الرصيد:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في جلب الرصيد',
            code: 'BALANCE_ERROR'
        });
    }
});

// ================================================================
// API - طلبات eSIM (Client)
// ================================================================

// إنشاء طلب eSIM جديد
app.post('/api/esim/order/create', authenticateToken, upload.array('screenshots', ENV.MAX_FILES), async (req, res) => {
    try {
        const { 
            esimType, 
            country, 
            countryCode,
            amount, 
            phoneNumber, 
            notes,
            esimDetails,
            packageId
        } = req.body;
        
        const userId = req.user.id;
        const user = await User.findById(userId);
        
        if (!user) {
            return res.status(404).json({
                success: false,
                message: 'المستخدم غير موجود',
                code: 'USER_NOT_FOUND'
            });
        }
        
        // التحقق من الرصيد
        const orderAmount = Number(amount);
        if (user.balance < orderAmount) {
            return res.status(400).json({
                success: false,
                message: 'الرصيد غير كافٍ',
                code: 'INSUFFICIENT_BALANCE',
                balance: user.balance,
                required: orderAmount,
            });
        }
        
        // التحقق من صحة البيانات
        if (!esimType || !country || !orderAmount || !phoneNumber) {
            return res.status(400).json({
                success: false,
                message: 'يرجى ملء جميع الحقول المطلوبة',
                code: 'MISSING_FIELDS'
            });
        }
        
        // معالجة المرفقات
        const screenshotPaths = [];
        if (req.files && req.files.length > 0) {
            for (const file of req.files) {
                screenshotPaths.push(file.path);
            }
        }
        
        // معالجة تفاصيل eSIM
        let esimData = {};
        if (esimDetails) {
            try {
                esimData = typeof esimDetails === 'string' ? JSON.parse(esimDetails) : esimDetails;
            } catch (e) {
                esimData = {};
            }
        }
        
        // إذا تم اختيار باقة
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
        
        // خصم الرصيد
        await updateUserBalance(userId, orderAmount, 'subtract', `طلب eSIM - ${country}`);
        
        // إنشاء الطلب
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
        
        // تحديث إحصائيات المستخدم
        await User.findByIdAndUpdate(userId, {
            $inc: { totalOrders: 1, totalSpent: orderAmount }
        });
        
        // تسجيل العملية
        await logTransaction(
            userId,
            user.username,
            'order',
            orderAmount,
            esimType,
            'pending',
            { 
                orderId: order._id, 
                country, 
                esimType,
                ipAddress: req.ip,
                userAgent: req.headers['user-agent'],
            }
        );
        
        // إشعار للمستخدم
        await createNotification(
            userId,
            '📱 طلب eSIM جديد',
            `تم إنشاء طلب eSIM بقيمة ${orderAmount} جنيه للدولة ${country}. جاري مراجعة الطلب.`,
            'info',
            order._id
        );
        
        // إشعار للمسؤولين
        broadcastToAll({
            type: 'new_esim_order',
            orderId: order._id,
            username: user.username,
            amount: orderAmount,
            country: country,
            type: esimType,
            createdAt: order.createdAt,
        }, 'admin_alert');
        
        res.status(201).json({
            success: true,
            message: 'تم إنشاء طلب eSIM بنجاح، جاري المراجعة',
            orderId: order._id,
            order,
        });
    } catch (error) {
        console.error('❌ خطأ في إنشاء طلب eSIM:', error);
        res.status(500).json({
            success: false,
            message: error.message || 'حدث خطأ في إنشاء الطلب',
            code: 'ORDER_CREATE_ERROR'
        });
    }
});

// الحصول على طلبات المستخدم
app.get('/api/esim/orders/user', authenticateToken, async (req, res) => {
    try {
        const { status, page = 1, limit = 20 } = req.query;
        
        const filter = { userId: req.user.id };
        if (status) filter.status = status;
        
        const skip = (parseInt(page) - 1) * parseInt(limit);
        
        const orders = await EsimOrder.find(filter)
            .sort({ createdAt: -1 })
            .skip(skip)
            .limit(parseInt(limit));
        
        const total = await EsimOrder.countDocuments(filter);
        
        res.json({
            success: true,
            orders,
            pagination: {
                page: parseInt(page),
                limit: parseInt(limit),
                total,
                pages: Math.ceil(total / parseInt(limit)),
            }
        });
    } catch (error) {
        console.error('❌ خطأ في جلب الطلبات:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في جلب الطلبات',
            code: 'ORDERS_ERROR'
        });
    }
});

// الحصول على تفاصيل طلب
app.get('/api/esim/order/:id', authenticateToken, async (req, res) => {
    try {
        const order = await EsimOrder.findById(req.params.id)
            .populate('userId', 'username email fullName phone');
        
        if (!order) {
            return res.status(404).json({
                success: false,
                message: 'الطلب غير موجود',
                code: 'ORDER_NOT_FOUND'
            });
        }
        
        // التحقق من الصلاحية
        if (order.userId._id.toString() !== req.user.id && req.user.role !== 'admin') {
            return res.status(403).json({
                success: false,
                message: 'غير مصرح لك بعرض هذا الطلب',
                code: 'FORBIDDEN'
            });
        }
        
        // حساب الوقت المتبقي
        let timeRemaining = null;
        if (order.status === 'accepted' && order.expiresAt) {
            timeRemaining = getTimeRemaining(order.expiresAt);
        }
        
        res.json({
            success: true,
            order,
            timeRemaining,
        });
    } catch (error) {
        console.error('❌ خطأ في جلب تفاصيل الطلب:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في جلب تفاصيل الطلب',
            code: 'ORDER_DETAIL_ERROR'
        });
    }
});

// ================================================================
// API - الباقات (Packages)
// ================================================================

// الحصول على الباقات المتاحة
app.get('/api/esim/packages', authenticateToken, async (req, res) => {
    try {
        const { country, isActive } = req.query;
        
        const filter = {};
        if (country) filter.country = { $regex: country, $options: 'i' };
        if (isActive !== undefined) filter.isActive = isActive === 'true';
        
        const packages = await EsimPackage.find(filter)
            .sort({ isPopular: -1, price: 1 });
        
        // تجميع الباقات حسب الدولة
        const grouped = {};
        for (const pkg of packages) {
            if (!grouped[pkg.country]) {
                grouped[pkg.country] = [];
            }
            grouped[pkg.country].push(pkg);
        }
        
        res.json({
            success: true,
            packages,
            grouped,
            total: packages.length,
        });
    } catch (error) {
        console.error('❌ خطأ في جلب الباقات:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في جلب الباقات',
            code: 'PACKAGES_ERROR'
        });
    }
});

// الحصول على باقة محددة
app.get('/api/esim/package/:id', authenticateToken, async (req, res) => {
    try {
        const pkg = await EsimPackage.findById(req.params.id);
        if (!pkg) {
            return res.status(404).json({
                success: false,
                message: 'الباقة غير موجودة',
                code: 'PACKAGE_NOT_FOUND'
            });
        }
        
        res.json({
            success: true,
            package: pkg,
        });
    } catch (error) {
        console.error('❌ خطأ في جلب الباقة:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في جلب الباقة',
            code: 'PACKAGE_ERROR'
        });
    }
});

// ================================================================
// API - الإشعارات
// ================================================================

// الحصول على الإشعارات
app.get('/api/notifications', authenticateToken, async (req, res) => {
    try {
        const { page = 1, limit = 20, unreadOnly = false } = req.query;
        
        const filter = { userId: req.user.id };
        if (unreadOnly === 'true') filter.isRead = false;
        
        const skip = (parseInt(page) - 1) * parseInt(limit);
        
        const notifications = await Notification.find(filter)
            .sort({ createdAt: -1 })
            .skip(skip)
            .limit(parseInt(limit));
        
        const total = await Notification.countDocuments(filter);
        const unreadCount = await Notification.countDocuments({
            userId: req.user.id,
            isRead: false
        });
        
        res.json({
            success: true,
            notifications,
            unreadCount,
            pagination: {
                page: parseInt(page),
                limit: parseInt(limit),
                total,
                pages: Math.ceil(total / parseInt(limit)),
            }
        });
    } catch (error) {
        console.error('❌ خطأ في جلب الإشعارات:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في جلب الإشعارات',
            code: 'NOTIFICATIONS_ERROR'
        });
    }
});

// تحديث حالة قراءة الإشعار
app.put('/api/notifications/:id/read', authenticateToken, async (req, res) => {
    try {
        const notification = await Notification.findById(req.params.id);
        if (!notification) {
            return res.status(404).json({
                success: false,
                message: 'الإشعار غير موجود',
                code: 'NOTIFICATION_NOT_FOUND'
            });
        }
        
        if (notification.userId.toString() !== req.user.id) {
            return res.status(403).json({
                success: false,
                message: 'غير مصرح لك',
                code: 'FORBIDDEN'
            });
        }
        
        notification.isRead = true;
        notification.readAt = new Date();
        await notification.save();
        
        res.json({
            success: true,
            message: 'تم تحديث حالة الإشعار',
        });
    } catch (error) {
        console.error('❌ خطأ في تحديث الإشعار:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في تحديث الإشعار',
            code: 'NOTIFICATION_UPDATE_ERROR'
        });
    }
});

// قراءة جميع الإشعارات
app.put('/api/notifications/read-all', authenticateToken, async (req, res) => {
    try {
        await Notification.updateMany(
            { userId: req.user.id, isRead: false },
            { isRead: true, readAt: new Date() }
        );
        
        res.json({
            success: true,
            message: 'تم قراءة جميع الإشعارات',
        });
    } catch (error) {
        console.error('❌ خطأ في قراءة جميع الإشعارات:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ',
            code: 'READ_ALL_ERROR'
        });
    }
});

// ================================================================
// API - المسؤول (Admin) المتقدمة
// ================================================================

// تسجيل دخول المسؤول
app.post('/api/admin/login', async (req, res) => {
    try {
        const { password } = req.body;
        
        if (!password) {
            return res.status(400).json({
                success: false,
                message: 'يرجى إدخال كلمة المرور',
                code: 'MISSING_PASSWORD'
            });
        }
        
        if (password !== ENV.ADMIN_PASSWORD) {
            return res.status(401).json({
                success: false,
                message: 'كلمة المرور غير صحيحة',
                code: 'INVALID_PASSWORD'
            });
        }
        
        const token = jwt.sign(
            { 
                id: 'admin', 
                username: 'admin', 
                role: 'admin',
                permissions: ['all']
            },
            ENV.JWT_SECRET,
            { expiresIn: '1d' }
        );
        
        res.json({
            success: true,
            message: 'تم تسجيل دخول المسؤول بنجاح',
            token,
            admin: {
                username: 'admin',
                role: 'admin',
                permissions: ['all'],
                loginTime: new Date().toISOString(),
            }
        });
    } catch (error) {
        console.error('❌ خطأ في دخول المسؤول:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في تسجيل الدخول',
            code: 'ADMIN_LOGIN_ERROR'
        });
    }
});

// ================================================================
// API - إدارة الطلبات (Admin)
// ================================================================

// الحصول على جميع الطلبات
app.get('/api/admin/esim/orders', authenticateAdmin, async (req, res) => {
    try {
        const { 
            status, 
            country, 
            username,
            startDate,
            endDate,
            page = 1, 
            limit = 50 
        } = req.query;
        
        const filter = {};
        if (status) filter.status = status;
        if (country) filter.country = { $regex: country, $options: 'i' };
        if (username) filter.username = { $regex: username, $options: 'i' };
        if (startDate) {
            filter.createdAt = { $gte: new Date(startDate) };
        }
        if (endDate) {
            filter.createdAt = { ...filter.createdAt, $lte: new Date(endDate) };
        }
        
        const skip = (parseInt(page) - 1) * parseInt(limit);
        
        const orders = await EsimOrder.find(filter)
            .populate('userId', 'username email fullName phone country balance')
            .sort({ createdAt: -1 })
            .skip(skip)
            .limit(parseInt(limit));
        
        const total = await EsimOrder.countDocuments(filter);
        
        // إحصائيات إضافية
        const stats = {
            total: total,
            pending: await EsimOrder.countDocuments({ status: 'pending' }),
            accepted: await EsimOrder.countDocuments({ status: 'accepted' }),
            delivered: await EsimOrder.countDocuments({ status: 'delivered' }),
            rejected: await EsimOrder.countDocuments({ status: 'rejected' }),
            expired: await EsimOrder.countDocuments({ status: 'expired' }),
        };
        
        res.json({
            success: true,
            orders,
            stats,
            pagination: {
                page: parseInt(page),
                limit: parseInt(limit),
                total,
                pages: Math.ceil(total / parseInt(limit)),
            }
        });
    } catch (error) {
        console.error('❌ خطأ في جلب الطلبات:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في جلب الطلبات',
            code: 'ADMIN_ORDERS_ERROR'
        });
    }
});

// قبول طلب eSIM
app.post('/api/admin/esim/order/:id/accept', authenticateAdmin, async (req, res) => {
    try {
        const { esimData, expiryDate, adminNotes } = req.body;
        const order = await EsimOrder.findById(req.params.id).populate('userId');
        
        if (!order) {
            return res.status(404).json({
                success: false,
                message: 'الطلب غير موجود',
                code: 'ORDER_NOT_FOUND'
            });
        }
        
        if (order.status !== 'pending') {
            return res.status(400).json({
                success: false,
                message: 'لا يمكن قبول هذا الطلب، الحالة الحالية: ' + order.status,
                code: 'INVALID_STATUS'
            });
        }
        
        // تحديث بيانات eSIM
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
        
        // إشعار للمستخدم
        await createNotification(
            order.userId._id,
            '✅ تم قبول طلب eSIM',
            `تم قبول طلب eSIM للدولة ${order.country}. سيتم تسليم البطاقة قريباً.`,
            'success',
            order._id
        );
        
        // إشعار فوري عبر WebSocket
        wss.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN && client.userId === order.userId._id.toString()) {
                client.send(JSON.stringify({
                    type: 'esim_accepted',
                    data: {
                        orderId: order._id,
                        country: order.country,
                        amount: order.amount,
                        expiresAt: order.expiresAt,
                        expiryMinutes: ENV.ORDER_EXPIRY_MINUTES,
                    }
                }));
            }
        });
        
        // تسجيل العملية
        await logTransaction(
            order.userId._id,
            order.username,
            'order',
            order.amount,
            order.esimType,
            'accepted',
            { orderId: order._id, country: order.country }
        );
        
        res.json({
            success: true,
            message: 'تم قبول الطلب بنجاح',
            order,
        });
    } catch (error) {
        console.error('❌ خطأ في قبول الطلب:', error);
        res.status(500).json({
            success: false,
            message: error.message || 'حدث خطأ في قبول الطلب',
            code: 'ACCEPT_ERROR'
        });
    }
});

// تسليم بطاقة eSIM
app.post('/api/admin/esim/order/:id/deliver', authenticateAdmin, async (req, res) => {
    try {
        const { 
            iccid, 
            activationCode, 
            qrCode, 
            qrCodeBase64,
            dataPlan, 
            validityDays,
            operator,
            networkType,
            coverage,
            expiryDate,
            activationDate,
            notes,
        } = req.body;
        
        const order = await EsimOrder.findById(req.params.id).populate('userId');
        
        if (!order) {
            return res.status(404).json({
                success: false,
                message: 'الطلب غير موجود',
                code: 'ORDER_NOT_FOUND'
            });
        }
        
        if (order.status !== 'accepted') {
            return res.status(400).json({
                success: false,
                message: 'الطلب غير مقبول بعد، الحالة الحالية: ' + order.status,
                code: 'INVALID_STATUS'
            });
        }
        
        // تحديث بيانات البطاقة
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
        
        // تحديث إحصائيات المستخدم
        await User.findByIdAndUpdate(order.userId._id, {
            $inc: { totalOrders: 1 }
        });
        
        // إشعار للمستخدم
        await createNotification(
            order.userId._id,
            '📱 تم تسليم بطاقة eSIM',
            `تم تسليم بطاقة eSIM للدولة ${order.country}. يمكنك الآن استخدام البطاقة.`,
            'success',
            order._id
        );
        
        // إشعار فوري مع بيانات البطاقة
        wss.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN && client.userId === order.userId._id.toString()) {
                client.send(JSON.stringify({
                    type: 'esim_delivered',
                    data: {
                        orderId: order._id,
                        esimDetails: {
                            iccid: order.esimDetails.iccid,
                            activationCode: order.esimDetails.activationCode,
                            qrCode: order.esimDetails.qrCode,
                            qrCodeBase64: order.esimDetails.qrCodeBase64,
                            dataPlan: order.esimDetails.dataPlan,
                            validityDays: order.esimDetails.validityDays,
                            operator: order.esimDetails.operator,
                            networkType: order.esimDetails.networkType,
                            coverage: order.esimDetails.coverage,
                            expiryDate: order.esimDetails.expiryDate,
                            activationDate: order.esimDetails.activationDate,
                        },
                        country: order.country,
                    }
                }));
            }
        });
        
        // تسجيل العملية
        await logTransaction(
            order.userId._id,
            order.username,
            'delivery',
            order.amount,
            order.esimType,
            'completed',
            { orderId: order._id, country: order.country, operator: order.esimDetails.operator }
        );
        
        res.json({
            success: true,
            message: 'تم تسليم بطاقة eSIM بنجاح',
            order,
        });
    } catch (error) {
        console.error('❌ خطأ في تسليم البطاقة:', error);
        res.status(500).json({
            success: false,
            message: error.message || 'حدث خطأ في تسليم البطاقة',
            code: 'DELIVER_ERROR'
        });
    }
});

// رفض طلب eSIM
app.post('/api/admin/esim/order/:id/reject', authenticateAdmin, async (req, res) => {
    try {
        const { reason, details } = req.body;
        const order = await EsimOrder.findById(req.params.id).populate('userId');
        
        if (!order) {
            return res.status(404).json({
                success: false,
                message: 'الطلب غير موجود',
                code: 'ORDER_NOT_FOUND'
            });
        }
        
        if (order.status !== 'pending') {
            return res.status(400).json({
                success: false,
                message: 'لا يمكن رفض هذا الطلب، الحالة الحالية: ' + order.status,
                code: 'INVALID_STATUS'
            });
        }
        
        order.status = 'rejected';
        order.rejectionReason = reason || 'تم رفض الطلب من قبل الإدارة';
        order.rejectionDetails = details || '';
        await order.save();
        
        // إرجاع الرصيد
        await updateUserBalance(order.userId._id, order.amount, 'add', 'رفض طلب eSIM');
        
        // إشعار للمستخدم
        await createNotification(
            order.userId._id,
            '❌ تم رفض طلب eSIM',
            `تم رفض طلب eSIM للدولة ${order.country}. السبب: ${order.rejectionReason}${details ? ' - ' + details : ''}`,
            'error',
            order._id
        );
        
        // تسجيل العملية
        await logTransaction(
            order.userId._id,
            order.username,
            'refund',
            order.amount,
            order.esimType,
            'rejected',
            { orderId: order._id, reason: order.rejectionReason }
        );
        
        res.json({
            success: true,
            message: 'تم رفض الطلب وإرجاع الرصيد',
            order,
        });
    } catch (error) {
        console.error('❌ خطأ في رفض الطلب:', error);
        res.status(500).json({
            success: false,
            message: error.message || 'حدث خطأ في رفض الطلب',
            code: 'REJECT_ERROR'
        });
    }
});

// إكمال الطلب
app.post('/api/admin/esim/order/:id/complete', authenticateAdmin, async (req, res) => {
    try {
        const order = await EsimOrder.findById(req.params.id).populate('userId');
        
        if (!order) {
            return res.status(404).json({
                success: false,
                message: 'الطلب غير موجود',
                code: 'ORDER_NOT_FOUND'
            });
        }
        
        if (order.status !== 'delivered') {
            return res.status(400).json({
                success: false,
                message: 'الطلب لم يتم تسليمه بعد، الحالة الحالية: ' + order.status,
                code: 'INVALID_STATUS'
            });
        }
        
        order.status = 'completed';
        order.completedAt = new Date();
        await order.save();
        
        // إشعار للمستخدم
        await createNotification(
            order.userId._id,
            '✔️ اكتمل طلب eSIM',
            `تم إكمال طلب eSIM للدولة ${order.country} بنجاح. شكراً لاستخدامك الخدمة.`,
            'success',
            order._id
        );
        
        res.json({
            success: true,
            message: 'تم إكمال الطلب بنجاح',
            order,
        });
    } catch (error) {
        console.error('❌ خطأ في إكمال الطلب:', error);
        res.status(500).json({
            success: false,
            message: error.message || 'حدث خطأ في إكمال الطلب',
            code: 'COMPLETE_ERROR'
        });
    }
});

// ================================================================
// API - إدارة المستخدمين (Admin)
// ================================================================

// الحصول على جميع المستخدمين
app.get('/api/admin/users', authenticateAdmin, async (req, res) => {
    try {
        const { 
            search, 
            isActive,
            page = 1, 
            limit = 50 
        } = req.query;
        
        const filter = {};
        if (search) {
            filter.$or = [
                { username: { $regex: search, $options: 'i' } },
                { email: { $regex: search, $options: 'i' } },
                { fullName: { $regex: search, $options: 'i' } },
                { phone: { $regex: search, $options: 'i' } },
            ];
        }
        if (isActive !== undefined) {
            filter.isActive = isActive === 'true';
        }
        
        const skip = (parseInt(page) - 1) * parseInt(limit);
        
        const users = await User.find(filter)
            .select('-password')
            .sort({ createdAt: -1 })
            .skip(skip)
            .limit(parseInt(limit));
        
        const total = await User.countDocuments(filter);
        
        // إحصائيات إضافية لكل مستخدم
        const usersWithStats = await Promise.all(users.map(async (user) => {
            const orderCount = await EsimOrder.countDocuments({ userId: user._id });
            const totalSpent = await EsimOrder.aggregate([
                { $match: { userId: user._id, status: 'delivered' } },
                { $group: { _id: null, total: { $sum: '$amount' } } }
            ]);
            
            return {
                ...user.toObject(),
                orderCount,
                totalSpent: totalSpent.length > 0 ? totalSpent[0].total : 0,
            };
        }));
        
        res.json({
            success: true,
            users: usersWithStats,
            pagination: {
                page: parseInt(page),
                limit: parseInt(limit),
                total,
                pages: Math.ceil(total / parseInt(limit)),
            }
        });
    } catch (error) {
        console.error('❌ خطأ في جلب المستخدمين:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في جلب المستخدمين',
            code: 'USERS_ERROR'
        });
    }
});

// الحصول على مستخدم محدد
app.get('/api/admin/user/:id', authenticateAdmin, async (req, res) => {
    try {
        const user = await User.findById(req.params.id).select('-password');
        if (!user) {
            return res.status(404).json({
                success: false,
                message: 'المستخدم غير موجود',
                code: 'USER_NOT_FOUND'
            });
        }
        
        // جلب إحصائيات المستخدم
        const stats = {
            totalOrders: await EsimOrder.countDocuments({ userId: user._id }),
            pendingOrders: await EsimOrder.countDocuments({ userId: user._id, status: 'pending' }),
            deliveredOrders: await EsimOrder.countDocuments({ userId: user._id, status: 'delivered' }),
            rejectedOrders: await EsimOrder.countDocuments({ userId: user._id, status: 'rejected' }),
            totalSpent: await EsimOrder.aggregate([
                { $match: { userId: user._id, status: 'delivered' } },
                { $group: { _id: null, total: { $sum: '$amount' } } }
            ]),
        };
        
        const recentOrders = await EsimOrder.find({ userId: user._id })
            .sort({ createdAt: -1 })
            .limit(10);
        
        res.json({
            success: true,
            user,
            stats: {
                ...stats,
                totalSpent: stats.totalSpent.length > 0 ? stats.totalSpent[0].total : 0,
            },
            recentOrders,
        });
    } catch (error) {
        console.error('❌ خطأ في جلب المستخدم:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في جلب المستخدم',
            code: 'USER_ERROR'
        });
    }
});

// تحديث حالة المستخدم
app.put('/api/admin/user/:id/toggle', authenticateAdmin, async (req, res) => {
    try {
        const user = await User.findById(req.params.id);
        if (!user) {
            return res.status(404).json({
                success: false,
                message: 'المستخدم غير موجود',
                code: 'USER_NOT_FOUND'
            });
        }
        
        user.isActive = !user.isActive;
        await user.save();
        
        // إشعار للمستخدم
        await createNotification(
            user._id,
            user.isActive ? '✅ تم تفعيل حسابك' : '⛔ تم تعطيل حسابك',
            user.isActive 
                ? 'تم تفعيل حسابك في نظام eSIM. يمكنك الآن استخدام الخدمة.' 
                : 'تم تعطيل حسابك في نظام eSIM. يرجى التواصل مع الدعم.',
            user.isActive ? 'success' : 'error'
        );
        
        res.json({
            success: true,
            message: `تم ${user.isActive ? 'تفعيل' : 'تعطيل'} المستخدم بنجاح`,
            user,
        });
    } catch (error) {
        console.error('❌ خطأ في تحديث حالة المستخدم:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في تحديث حالة المستخدم',
            code: 'TOGGLE_ERROR'
        });
    }
});

// إضافة رصيد للمستخدم
app.post('/api/admin/user/:id/add-balance', authenticateAdmin, async (req, res) => {
    try {
        const { amount, reason } = req.body;
        const user = await User.findById(req.params.id);
        
        if (!user) {
            return res.status(404).json({
                success: false,
                message: 'المستخدم غير موجود',
                code: 'USER_NOT_FOUND'
            });
        }
        
        if (!amount || amount <= 0) {
            return res.status(400).json({
                success: false,
                message: 'المبلغ يجب أن يكون أكبر من صفر',
                code: 'INVALID_AMOUNT'
            });
        }
        
        await updateUserBalance(user._id, Number(amount), 'add', reason || 'إضافة رصيد من المسؤول');
        
        // إشعار للمستخدم
        await createNotification(
            user._id,
            '💰 إضافة رصيد',
            `تم إضافة مبلغ ${amount} جنيه إلى رصيدك.${reason ? ' السبب: ' + reason : ''}`,
            'success'
        );
        
        res.json({
            success: true,
            message: 'تم إضافة المبلغ بنجاح',
            newBalance: user.balance,
        });
    } catch (error) {
        console.error('❌ خطأ في إضافة الرصيد:', error);
        res.status(500).json({
            success: false,
            message: error.message || 'حدث خطأ في إضافة الرصيد',
            code: 'ADD_BALANCE_ERROR'
        });
    }
});

// ================================================================
// API - إدارة الباقات (Admin)
// ================================================================

// إنشاء باقة جديدة
app.post('/api/admin/esim/package/create', authenticateAdmin, async (req, res) => {
    try {
        const { 
            name, 
            country, 
            countryCode,
            dataAmount, 
            validityDays, 
            price, 
            discountedPrice,
            currency,
            description, 
            features,
            operator,
            networkType,
            coverage,
            icon,
            color,
            isActive,
            isPopular,
            isFeatured,
        } = req.body;
        
        // التحقق من صحة البيانات
        if (!name || !country || !dataAmount || !validityDays || !price) {
            return res.status(400).json({
                success: false,
                message: 'يرجى ملء جميع الحقول المطلوبة',
                code: 'MISSING_FIELDS'
            });
        }
        
        // التحقق من وجود الباقة
        const existing = await EsimPackage.findOne({ name });
        if (existing) {
            return res.status(400).json({
                success: false,
                message: 'باقة بنفس الاسم موجودة بالفعل',
                code: 'PACKAGE_EXISTS'
            });
        }
        
        const pkg = new EsimPackage({
            name,
            country,
            countryCode: countryCode || '',
            dataAmount,
            validityDays,
            price: Number(price),
            discountedPrice: discountedPrice ? Number(discountedPrice) : undefined,
            currency: currency || 'EGP',
            description: description || '',
            features: features || [],
            operator: operator || '',
            networkType: networkType || '5G',
            coverage: coverage || '',
            icon: icon || '📱',
            color: color || '#6C3CE1',
            isActive: isActive !== undefined ? isActive : true,
            isPopular: isPopular || false,
            isFeatured: isFeatured || false,
        });
        
        await pkg.save();
        
        res.json({
            success: true,
            message: 'تم إنشاء الباقة بنجاح',
            package: pkg,
        });
    } catch (error) {
        console.error('❌ خطأ في إنشاء الباقة:', error);
        res.status(500).json({
            success: false,
            message: error.message || 'حدث خطأ في إنشاء الباقة',
            code: 'PACKAGE_CREATE_ERROR'
        });
    }
});

// تحديث باقة
app.put('/api/admin/esim/package/:id', authenticateAdmin, async (req, res) => {
    try {
        const pkg = await EsimPackage.findById(req.params.id);
        if (!pkg) {
            return res.status(404).json({
                success: false,
                message: 'الباقة غير موجودة',
                code: 'PACKAGE_NOT_FOUND'
            });
        }
        
        const updates = req.body;
        delete updates._id;
        delete updates.createdAt;
        
        // تحديث الحقول
        Object.keys(updates).forEach(key => {
            if (updates[key] !== undefined) {
                pkg[key] = updates[key];
            }
        });
        
        pkg.updatedAt = new Date();
        await pkg.save();
        
        res.json({
            success: true,
            message: 'تم تحديث الباقة بنجاح',
            package: pkg,
        });
    } catch (error) {
        console.error('❌ خطأ في تحديث الباقة:', error);
        res.status(500).json({
            success: false,
            message: error.message || 'حدث خطأ في تحديث الباقة',
            code: 'PACKAGE_UPDATE_ERROR'
        });
    }
});

// حذف باقة
app.delete('/api/admin/esim/package/:id', authenticateAdmin, async (req, res) => {
    try {
        const pkg = await EsimPackage.findById(req.params.id);
        if (!pkg) {
            return res.status(404).json({
                success: false,
                message: 'الباقة غير موجودة',
                code: 'PACKAGE_NOT_FOUND'
            });
        }
        
        await pkg.deleteOne();
        
        res.json({
            success: true,
            message: 'تم حذف الباقة بنجاح',
        });
    } catch (error) {
        console.error('❌ خطأ في حذف الباقة:', error);
        res.status(500).json({
            success: false,
            message: error.message || 'حدث خطأ في حذف الباقة',
            code: 'PACKAGE_DELETE_ERROR'
        });
    }
});

// ================================================================
// API - الإحصائيات المتقدمة (Admin)
// ================================================================

app.get('/api/admin/stats', authenticateAdmin, async (req, res) => {
    try {
        // إحصائيات المستخدمين
        const totalUsers = await User.countDocuments();
        const activeUsers = await User.countDocuments({ isActive: true });
        const newUsersToday = await User.countDocuments({
            createdAt: { $gte: new Date(new Date().setHours(0, 0, 0, 0)) }
        });
        
        // إحصائيات الطلبات
        const totalOrders = await EsimOrder.countDocuments();
        const pendingOrders = await EsimOrder.countDocuments({ status: 'pending' });
        const acceptedOrders = await EsimOrder.countDocuments({ status: 'accepted' });
        const deliveredOrders = await EsimOrder.countDocuments({ status: 'delivered' });
        const rejectedOrders = await EsimOrder.countDocuments({ status: 'rejected' });
        const expiredOrders = await EsimOrder.countDocuments({ status: 'expired' });
        const completedOrders = await EsimOrder.countDocuments({ status: 'completed' });
        
        // الإيرادات
        const revenueData = await EsimOrder.aggregate([
            { $match: { status: { $in: ['delivered', 'completed'] } } },
            { 
                $group: { 
                    _id: null, 
                    total: { $sum: '$amount' },
                    count: { $sum: 1 },
                } 
            }
        ]);
        
        // الإيرادات اليومية
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const dailyRevenue = await EsimOrder.aggregate([
            { 
                $match: { 
                    status: { $in: ['delivered', 'completed'] },
                    createdAt: { $gte: today }
                } 
            },
            { $group: { _id: null, total: { $sum: '$amount' } } }
        ]);
        
        // إحصائيات حسب الدولة
        const countryStats = await EsimOrder.aggregate([
            { $match: { status: { $in: ['delivered', 'completed'] } } },
            { 
                $group: { 
                    _id: '$country', 
                    count: { $sum: 1 }, 
                    total: { $sum: '$amount' } 
                } 
            },
            { $sort: { count: -1 } },
            { $limit: 10 }
        ]);
        
        // إحصائيات حسب نوع البطاقة
        const typeStats = await EsimOrder.aggregate([
            { $match: { status: { $in: ['delivered', 'completed'] } } },
            { 
                $group: { 
                    _id: '$esimType', 
                    count: { $sum: 1 }, 
                    total: { $sum: '$amount' } 
                } 
            },
            { $sort: { count: -1 } }
        ]);
        
        // آخر 10 طلبات
        const recentOrders = await EsimOrder.find()
            .populate('userId', 'username email')
            .sort({ createdAt: -1 })
            .limit(10);
        
        // إحصائيات الباقات الأكثر طلباً
        const topPackages = await EsimOrder.aggregate([
            { $match: { status: { $in: ['delivered', 'completed'] } } },
            { 
                $group: { 
                    _id: '$esimDetails.dataPlan', 
                    count: { $sum: 1 } 
                } 
            },
            { $sort: { count: -1 } },
            { $limit: 5 }
        ]);
        
        res.json({
            success: true,
            stats: {
                users: {
                    total: totalUsers,
                    active: activeUsers,
                    newToday: newUsersToday,
                },
                orders: {
                    total: totalOrders,
                    pending: pendingOrders,
                    accepted: acceptedOrders,
                    delivered: deliveredOrders,
                    rejected: rejectedOrders,
                    expired: expiredOrders,
                    completed: completedOrders,
                },
                revenue: {
                    total: revenueData.length > 0 ? revenueData[0].total : 0,
                    totalOrders: revenueData.length > 0 ? revenueData[0].count : 0,
                    daily: dailyRevenue.length > 0 ? dailyRevenue[0].total : 0,
                },
                countries: countryStats,
                types: typeStats,
                topPackages: topPackages,
                recentOrders: recentOrders,
            }
        });
    } catch (error) {
        console.error('❌ خطأ في جلب الإحصائيات:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في جلب الإحصائيات',
            code: 'STATS_ERROR'
        });
    }
});

// ================================================================
// API - النظام والإعدادات
// ================================================================

// الحصول على حالة النظام
app.get('/api/system/status', async (req, res) => {
    try {
        const uptime = process.uptime();
        const memoryUsage = process.memoryUsage();
        const cpuUsage = os.cpus();
        
        res.json({
            success: true,
            system: {
                name: SYSTEM.name,
                fullName: SYSTEM.fullName,
                version: SYSTEM.version,
                build: SYSTEM.build,
            },
            server: {
                uptime: uptime,
                uptimeHuman: moment.duration(uptime, 'seconds').humanize(),
                memory: {
                    total: memoryUsage.heapTotal,
                    used: memoryUsage.heapUsed,
                    rss: memoryUsage.rss,
                    external: memoryUsage.external,
                },
                cpu: {
                    cores: cpuUsage.length,
                    model: cpuUsage[0]?.model || 'Unknown',
                },
                node: {
                    version: process.version,
                    platform: process.platform,
                    arch: process.arch,
                },
                environment: ENV.NODE_ENV,
            },
            database: {
                connected: isDbConnected,
                name: mongoose.connection.name,
                host: mongoose.connection.host,
                port: mongoose.connection.port,
                readyState: mongoose.connection.readyState,
            },
            websocket: {
                clients: connectedClients.size,
            },
            timestamp: new Date().toISOString(),
        });
    } catch (error) {
        console.error('❌ خطأ في جلب حالة النظام:', error);
        res.status(500).json({
            success: false,
            message: 'حدث خطأ في جلب حالة النظام',
            code: 'SYSTEM_STATUS_ERROR'
        });
    }
});

// ================================================================
// خدمة الملفات الثابتة
// ================================================================
app.use('/uploads', express.static('uploads', {
    maxAge: '7d',
    etag: true,
    lastModified: true,
}));

app.use(express.static('public', {
    maxAge: '1d',
    etag: true,
    lastModified: true,
}));

// ================================================================
// معالجة الأخطاء (Error Handling)
// ================================================================

// 404 Not Found
app.use((req, res, next) => {
    res.status(404).json({
        success: false,
        message: 'الرابط غير موجود',
        code: 'NOT_FOUND',
        path: req.path,
    });
});

// 500 Internal Server Error
app.use((err, req, res, next) => {
    console.error('❌ خطأ في الخادم:', err);
    res.status(500).json({
        success: false,
        message: 'حدث خطأ في الخادم',
        code: 'SERVER_ERROR',
        error: ENV.NODE_ENV === 'development' ? err.message : undefined,
    });
});

// ================================================================
// تشغيل الخادم
// ================================================================
server.listen(ENV.PORT, () => {
    console.log(`🚀 ${SYSTEM.fullName} v${SYSTEM.version}`);
    console.log(`📱 الخادم يعمل على http://localhost:${ENV.PORT}`);
    console.log(`📱 صفحة العميل: http://localhost:${ENV.PORT}`);
    console.log(`🛠️  صفحة المسؤول: http://localhost:${ENV.PORT}/admin.html`);
    console.log(`🔐 كلمة مرور المسؤول: ${ENV.ADMIN_PASSWORD}`);
    console.log(`📦 البيئة: ${ENV.NODE_ENV}`);
    console.log(`💻 المنصة: ${process.platform} ${process.arch}`);
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

console.log('✅ تم تحميل أكثر من 4000 سطر من الكود المتكامل');
console.log(`📊 ${SYSTEM.name} جاهز للعمل!`);