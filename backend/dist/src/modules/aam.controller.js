"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __param = (this && this.__param) || function (paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.AamController = void 0;
const common_1 = require("@nestjs/common");
const aam_service_1 = require("./aam.service");
const dto_1 = require("./dto");
let AamController = class AamController {
    aam;
    constructor(aam) {
        this.aam = aam;
    }
    dashboard() {
        return this.aam.dashboard();
    }
    compliance() {
        return this.aam.compliance();
    }
    painTrends() {
        return this.aam.painTrends();
    }
    mealPlans(range) {
        return this.aam.mealPlans(range);
    }
    recipes() {
        return this.aam.recipes();
    }
    vegetables() {
        return this.aam.vegetables();
    }
    medicines() {
        return this.aam.medicines();
    }
    reminders() {
        return this.aam.reminders();
    }
    groceries(period = 'weekly') {
        return this.aam.groceries(period);
    }
    alerts() {
        return this.aam.alerts();
    }
    monthlyHealth() {
        return this.aam.monthlyHealth();
    }
    addHealthLog(dto) {
        return this.aam.addHealthLog(dto);
    }
    reports() {
        return this.aam.reports();
    }
    users() {
        return this.aam.users();
    }
};
exports.AamController = AamController;
__decorate([
    (0, common_1.Get)('dashboard'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", void 0)
], AamController.prototype, "dashboard", null);
__decorate([
    (0, common_1.Get)('compliance'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", void 0)
], AamController.prototype, "compliance", null);
__decorate([
    (0, common_1.Get)('pain-trends'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", void 0)
], AamController.prototype, "painTrends", null);
__decorate([
    (0, common_1.Get)('meal-plans/:range'),
    __param(0, (0, common_1.Param)('range')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", void 0)
], AamController.prototype, "mealPlans", null);
__decorate([
    (0, common_1.Get)('recipes'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", void 0)
], AamController.prototype, "recipes", null);
__decorate([
    (0, common_1.Get)('vegetables'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", void 0)
], AamController.prototype, "vegetables", null);
__decorate([
    (0, common_1.Get)('medicines'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", void 0)
], AamController.prototype, "medicines", null);
__decorate([
    (0, common_1.Get)('reminders'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", void 0)
], AamController.prototype, "reminders", null);
__decorate([
    (0, common_1.Get)('groceries'),
    __param(0, (0, common_1.Query)('period')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", void 0)
], AamController.prototype, "groceries", null);
__decorate([
    (0, common_1.Get)('alerts'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", void 0)
], AamController.prototype, "alerts", null);
__decorate([
    (0, common_1.Get)('monthly-health'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", void 0)
], AamController.prototype, "monthlyHealth", null);
__decorate([
    (0, common_1.Post)('health-logs'),
    __param(0, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [dto_1.HealthLogDto]),
    __metadata("design:returntype", void 0)
], AamController.prototype, "addHealthLog", null);
__decorate([
    (0, common_1.Get)('reports'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", void 0)
], AamController.prototype, "reports", null);
__decorate([
    (0, common_1.Get)('admin/users'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", void 0)
], AamController.prototype, "users", null);
exports.AamController = AamController = __decorate([
    (0, common_1.Controller)(),
    __metadata("design:paramtypes", [aam_service_1.AamService])
], AamController);
//# sourceMappingURL=aam.controller.js.map