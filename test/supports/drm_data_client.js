'use strict';

const Base = require('sdk-base');
const debug = require('debug')('drm:dataClient');
const assert = require('assert');
const {
  AttributeGetRequest,
  SubscriberRegResult,
  AttributeSetRequest,
} = require('@alipay/tr-protocol');
const Server = require('./server');
const DrmCache = require('./cache/drm_cache');
const BoltClient = require('./remoting/bolt_client');
const RegManager = require('./reg_manager');
const ValueManager = require('./value_manager');
const HeartbeatManager = require('./heartbeat_manager');
const AttributeGetRequestHandler = require('./remoting/attribute_get_request_handler');
const AttributeSetRequestHandler = require('./remoting/attribute_set_request_handler');
const SubscriberRegResultHandler = require('./remoting/subscriber_reg_result_handler');
const ZdrmdataAddressPool = require('./address_pool');

const defaultOptions = {
  configclient: null,
  timeout: 5000,
  logger: console,
  enableLocalCache: false,
  heartbeatTimeout: 30 * 1000, // 默认心跳频率.
  registerCheckInterval: 30 * 1000, // 默认检查注册信息间隔
  // zdrmdata.rest.url | http://zdrmdata-pool.${inner.domain} | drm资源rest接口
  // zdrmdataRestUrl: 'http://zdrmdata-pool.stable.alipay.net',
  enableQueryServer: false,
};

class DrmDataClient extends Base {
  /**
   * @class
   * @param {Object} options
   *  - {ConfigClient} configclient - 显示传入 configserver 客户端，用于服务寻址
   *  - {string} [zdrmdataRestUrl] - 获取 drm 数据的 url，用于兜底，只有无法动态获取 addressPool 的时候，才会使用该值
   *  - {object} [httpclient=require('urllib')] - 用于向 zdrmdataRestUrl 发送请求 rest 请求的工具类
   *  - {boolean} [antCloud] - 是否是金融云环境
   *  - {string} [accessKey] - 金融云 accessKey com.antcloud.mw.access
   *  - {string} [secretKey] - 金融云 secreyKey com.antcloud.mw.secret
   *  - {string} [instanceId] - 金融云 instanceId com.alipay.instanceid
   *  - {number} [cacheTime] - 金融云 cacheTime com.antcloud.mw.auth.cache.time
   *  - {string} [datacenter] - 金融云 datacenter com.alipay.ldc.datacenter
   *  - {string} [zone] - zone
   *  - {boolean} [enableLocalCache] -
   *  - {string} [cacheDir] -
   *  - {AntVipFinclount} [antvip] - 金融云 antVip 实例
   */
  constructor(options) {
    assert(options.httpclient, 'options.httpclient is required');
    assert(options.logger, 'options.logger is required');
    options = Object.assign({}, defaultOptions, options);
    options.initMethod = '_init';
    assert(options.antCloud || options.configclient, '主站需要开启 configclient');
    super(options);
    if (this.options.zdrmdataRestUrl) {
      // 'http://zdrmdata.stable.alipay.net////' = > 'http://zdrmdata.stable.alipay.net'
      this.options.zdrmdataRestUrl = this.options.zdrmdataRestUrl.replace(/\/+$/, '');
    }
    this._cache = new DrmCache();
    this._createHandlerMap();
    this.zdrmdataAddressPool = ZdrmdataAddressPool(options);
    this.boltClient = new BoltClient({
      logger: this.logger,
      handlerMap: this.handlerMap,
      zdrmDataAddressPool: this.zdrmdataAddressPool,
      cacheTime: this.options.cacheTime,
      accessKey: this.options.accessKey,
      secretKey: this.options.secretKey,
      instanceId: this.options.instanceId,
    });
    this.valueManager = new ValueManager({
      instanceId: this.options.instanceId,
      enableLocalCache: this.options.enableLocalCache,
      cacheDir: this.options.cacheDir,
      zdrmdataAddressPool: this.zdrmdataAddressPool,
      logger: this.logger,
      httpclient: this.httpclient,
    });
    this.valueManager.on('error', error => this.emit('error', error));
    this.regManager = new RegManager({
      logger: this.logger,
      boltClient: this.boltClient,
      keys: this.options.keys,
      valueManager: this.valueManager,
    });
    this.heartbeatManager = new HeartbeatManager({
      logger: this.logger,
      valueManager: this.valueManager,
      regManager: this.regManager,
      zdrmDataAddressPool: this.zdrmdataAddressPool,
      heartbeatTimeout: this.options.heartbeatTimeout,
      registerCheckInterval: this.options.registerCheckInterval,
      boltClient: this.boltClient,
    });
  }

  async _init() {
    if (this.options.enableQueryServer) {
      await this._createQueryServer();
    }
    await this.zdrmdataAddressPool.ready();
    debug('zdrmdata address pool ready');
    await this.boltClient.ready();
    debug('bolt client ready');
    await this.valueManager.ready();
    debug('value manager ready');
    this.heartbeatManager.start();
  }

  get logger() {
    return this.options.logger;
  }

  get httpclient() {
    return this.options.httpclient;
  }

  async _close() {
    return Promise.all([
      this.heartbeatManager.close(),
      this._server ? this._server.close() : Promise.resolve(),
      this.boltClient.close(),
    ]);
  }

  // 默认只能创建一个 server
  async _createQueryServer() {
    this._server = new Server({
      client: {
        get: req => {
          // 反查时返回推送的原始值，而不是 validate + parseValue 过的值
          let value = this.getRaw(req && req.dataId);

          if (typeof value === 'undefined') {
            // 如果返回 undefined, 返回到服务端会解析错误.
            value = '';
          }

          // 字符串的话, 不需要 stringify 了.
          if (typeof value !== 'string') {
            value = JSON.stringify(value);
          }

          this.logger.info('[drm:serverquery] req=%j, return=%j', req, value);
          return value;
        },
        set: (dataId, value, ipPort) => {
          const reg = this.regManager.dataIdMap[ dataId ];
          if (!reg) {
            this.logger.warn('[drm:serverpush:local] server %s push %s => %s, dataId have not register', ipPort, dataId, value);
            return;
          }
          this.valueManager.updateValue(reg, value);
          this.logger.info('[drm:serverpush:local] set %s => %j, get %j', dataId, value, this.get(dataId));
        },
      },
      logger: this.logger,
    });
    this._server.on('error', err => this.emit('error', err));
    await this._server.listen(this.options.port);
  }

  /**
   * DRM 资源订阅
   * @function DrmClient#subscribe
   * @param {Object} reg
   *   - {String} dataId
   *   - {String} groupId
   *   - {*} defaultValue 当没有推送或推送空值时的默认值
   * @param {Function} listener - 监听函数，调用时会收到 DRM 值
   * @return {DrmDataClient} this
   */
  subscribe(reg, listener) {
    this.regManager.subscribe(reg, () => {
      const cache = this.get(reg.dataId);
      listener(cache);
    });
    return this;
  }

  get(dataId) {
    const reg = this.regManager.getReg(dataId);
    return this.valueManager.get(reg);
  }

  // 为了 drm server 反查使用，需要返回推送的原始值，而不是 validate + parseValue 过的
  // 只允许使用 dataId 查询
  getRaw(dataId) {
    const rawValue = this.valueManager.getRawByDataId(dataId);
    return rawValue && rawValue.value;
  }

  updateValue(dataId, val) {
    const reg = this.regManager.dataIdMap[ dataId ];
    this.valueManager.updateValue(reg, val);
  }

  _createHandlerMap() {
    this.handlerMap = new Map([
      [
        AttributeGetRequest.prototype.className,
        new AttributeGetRequestHandler({
          client: this,
          logger: this.logger,
        }),
      ],
      [
        SubscriberRegResult.prototype.className,
        new SubscriberRegResultHandler({
          client: this,
          logger: this.logger,
        }),
      ],
      [
        AttributeSetRequest.prototype.className,
        new AttributeSetRequestHandler({
          client: this,
          logger: this.logger,
        }),
      ],
    ]);
  }
}

module.exports = DrmDataClient;
