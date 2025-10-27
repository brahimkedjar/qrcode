import { Body, Controller, Get, Param, Put, Post } from '@nestjs/common';
import { ArticleSetsService } from './article-sets.service';

@Controller('article-sets')
export class ArticleSetsController {
  constructor(private readonly svc: ArticleSetsService) {}

  @Get()
  list() {
    return this.svc.list();
  }

  @Get(':key')
  get(@Param('key') key: string) {
    const data = this.svc.get(key);
    return data || { name: key, articles: [] };
  }

  @Put(':key')
  put(@Param('key') key: string, @Body() body: any) {
    this.svc.put(key, { name: body?.name || key, articles: Array.isArray(body?.articles) ? body.articles : [] });
    return { ok: true };
  }

  @Post('import')
  import(@Body() body: any) {
    return this.svc.import(body);
  }
}

