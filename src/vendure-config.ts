import {
  VendureConfig,
  DefaultJobQueuePlugin,
  DefaultSchedulerPlugin,
  dummyPaymentHandler,
  facetValueCollectionFilter,
} from '@vendure/core';
import { ElasticsearchPlugin } from '@vendure/elasticsearch-plugin';
import {
  EmailPlugin,
  FileBasedTemplateLoader,
  defaultEmailHandlers,
} from '@vendure/email-plugin';
import { AssetServerPlugin } from '@vendure/asset-server-plugin';
import { AdminUiPlugin } from '@vendure/admin-ui-plugin';
import 'dotenv/config';
import path from 'path';

const IS_DEV = process.env.APP_ENV === 'dev';
const serverPort = +(process.env.PORT || 3000);

export const config: VendureConfig = {
  apiOptions: {
    port: serverPort,
    adminApiPath: 'admin-api',
    shopApiPath: 'shop-api',
    hostname: '0.0.0.0',
    trustProxy: 1,

    // CORS – permitir a storefront em duckdns
    cors: {
      origin: [
        'http://localhost:3000',             // opcional para dev local
        'https://carsandvibes.duckdns.org',  // frontend em produção
      ],
      credentials: true,
    },

    adminApiPlayground: {
      settings: {
        'request.credentials': 'include',
        'editor.theme': 'dark',
      },
    },
    adminApiDebug: true,
    shopApiPlayground: {
      settings: {
        'request.credentials': 'include',
        'editor.theme': 'dark',
      },
    },
    shopApiDebug: true,
  },

  authOptions: {
    // bearer para o Remix starter, cookie p/ admin & afins
    tokenMethod: ['bearer', 'cookie'],
    requireVerification: false,
    superadminCredentials: {
      identifier: 'superadmin',
      password: 'superadmin',
    },
    cookieOptions: {
      secret: process.env.COOKIE_SECRET || 'dev-cookie-secret',
      httpOnly: true,
      sameSite: 'lax',
      secure: false, // estás atrás de HTTP/SSL “terminado” antes do node
    },
  },

  dbConnectionOptions: {
    type: 'postgres',
    host: process.env.DB_HOST || 'localhost',
    port: +(process.env.DB_PORT || 5432),
    username: process.env.DB_USERNAME || 'vendure_user',
    password: process.env.DB_PASSWORD || 'cars123',
    database: process.env.DB_NAME || 'vendure',
    synchronize: IS_DEV,
    logging: false,
  },

  paymentOptions: {
    // dummy handler para testes
    paymentMethodHandlers: [dummyPaymentHandler],
  },

  catalogOptions: {
    collectionFilters: [facetValueCollectionFilter],
  },

  plugins: [
    AssetServerPlugin.init({
      route: 'assets',
      assetUploadDir: path.join(__dirname, '../static/assets'),
    }),

    DefaultJobQueuePlugin,
    DefaultSchedulerPlugin,

    ElasticsearchPlugin.init({
      indexPrefix: 'carsandvibes',
      clientOptions: {
        node: process.env.ELASTIC_NODE ?? 'http://127.0.0.1:9200',
      },
    }),

    EmailPlugin.init({
      devMode: true,
      route: 'mailbox',
      outputPath: path.join(__dirname, '../static/email/test-emails'),
      handlers: defaultEmailHandlers,
      templateLoader: new FileBasedTemplateLoader(
        path.join(__dirname, '../static/email/templates'),
      ),
    }),

    AdminUiPlugin.init({
      route: 'admin',
      port: serverPort,
    }),
  ],
};
