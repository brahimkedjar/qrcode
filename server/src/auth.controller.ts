import { Body, Controller, Post } from '@nestjs/common';
import { AuthService } from './auth.service';

@Controller('auth')
export class AuthController {
  constructor(private readonly auth: AuthService) {}

  @Post('login')
  async login(@Body() body: any) {
    const username = String(body?.username || body?.user || '').trim();
    const password = String(body?.password || '').trim();
    if (!username || !password) {
      return { ok: false, error: 'Missing username or password' };
    }

    try {
      const user = await this.auth.validateUser(username, password);
      const token = Buffer.from(`${username}:${Date.now()}`).toString('base64');
      return {
        ok: true,
        token,
        user: {
          name: user.displayName || username,
          username: user.username || username,
          email: user.email,
          dn: user.distinguishedName || undefined,
        },
      };
    } catch (e: any) {
      return { ok: false, error: e?.message || 'Authentication failed' };
    }
  }
}
