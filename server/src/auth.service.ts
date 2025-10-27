import { Injectable, UnauthorizedException } from '@nestjs/common';
import ldap from 'ldapjs';

export type LdapUser = {
  username: string;
  distinguishedName: string | null;
  email: string | null;
  displayName: string | null;
};

@Injectable()
export class AuthService {
  private readonly ldapHost: string;
  private readonly ldapDn: string;
  private readonly ldapDomain: string;

  constructor() {
    this.ldapHost = process.env.LDAP_URL || 'ldap://10.16.220.10:389';
    this.ldapDn = process.env.LDAP_BASE_DN || 'dc=corp,dc=anam,dc=dz';
    this.ldapDomain = process.env.LDAP_DOMAIN || 'corp.anam.dz';
    // eslint-disable-next-line no-console
    console.log(`Initialized with LDAP Host: ${this.ldapHost}, LDAP DN: ${this.ldapDn}`);
  }

  async validateUser(username: string, password: string): Promise<LdapUser> {
    // eslint-disable-next-line no-console
    console.log(`Attempting to validate user: ${username}`);
    // eslint-disable-next-line no-console
    console.log(`LDAP Host: ${this.ldapHost}, LDAP DN: ${this.ldapDn}`);

    const userDn = (process.env.LDAP_BIND_FORMAT || '{username}@' + this.ldapDomain).replace(
      '{username}',
      username,
    );
    // eslint-disable-next-line no-console
    console.log(`Binding with userDn: ${userDn}`);

    return new Promise<LdapUser>((resolve, reject) => {
      const client = ldap.createClient({
        url: this.ldapHost,
        reconnect: false,
        timeout: 8000,
        connectTimeout: 8000,
      });

      let settled = false;
      const done = (err?: Error, user?: LdapUser) => {
        if (settled) return;
        settled = true;
        try {
          client.unbind();
        } catch {}
        if (err) reject(err);
        else if (user) resolve(user);
        else reject(new UnauthorizedException('Authentification LDAP échouée'));
      };

      client.on('error', (err: any) => {
        // eslint-disable-next-line no-console
        console.error('LDAP Client Error:', err);
        done(new UnauthorizedException('Erreur de connexion au serveur LDAP: ' + err.message));
      });

      client.bind(userDn, password, (err: any) => {
        if (err) {
          // eslint-disable-next-line no-console
          console.error(`LDAP Bind Error: ${err.message}`);
          return done(new UnauthorizedException('Nom d’utilisateur ou mot de passe incorrect'));
        }
        // eslint-disable-next-line no-console
        console.log('LDAP Bind successful');

        client.search(
          this.ldapDn,
          {
            filter: `(sAMAccountName=${username})`,
            scope: 'sub',
            attributes: ['sAMAccountName', 'distinguishedName', 'mail', 'displayName'],
          },
          (err: any, res: any) => {
            if (err) {
              // eslint-disable-next-line no-console
              console.error(`LDAP Search Error: ${err.message}`);
              return done(new UnauthorizedException('Erreur lors de la recherche LDAP: ' + err.message));
            }

            let found = false;

            res.on('searchEntry', (entry: any) => {
              found = true;
              // entry.attributes is an array of { type, vals } in ldapjs
              const attributes: Record<string, string | null> = {};
              if (Array.isArray(entry?.attributes)) {
                for (const attr of entry.attributes) {
                  const type = attr.type;
                  const vals: any[] = (attr.vals || attr._vals || []).filter((v: any) => v !== undefined);
                  let value: any = null;
                  if (vals.length > 0) {
                    const v0 = vals[0];
                    value = Buffer.isBuffer(v0) ? v0.toString('utf8') : v0;
                  } else if (attr.value !== undefined) {
                    value = Buffer.isBuffer(attr.value) ? attr.value.toString('utf8') : attr.value;
                  }
                  attributes[type] = value ?? null;
                }
              } else if (entry?.object) {
                Object.assign(attributes, entry.object);
              }

              const user: LdapUser = {
                username: (attributes['sAMAccountName'] as string) || username,
                distinguishedName: (attributes['distinguishedName'] as string) || null,
                email: (attributes['mail'] as string) || `${username}@${this.ldapDomain}`,
                displayName: (attributes['displayName'] as string) || username,
              };
              // eslint-disable-next-line no-console
              console.log('LDAP Search Entry mapped:', user);
              done(undefined, user);
            });

            res.on('error', (err: Error) => {
              // eslint-disable-next-line no-console
              console.error('LDAP Search Response Error:', err);
              done(new UnauthorizedException('Erreur de réponse LDAP: ' + err.message));
            });

            res.on('end', () => {
              if (!found) {
                // eslint-disable-next-line no-console
                console.warn('LDAP Search completed without results');
                done(new UnauthorizedException('Utilisateur introuvable dans l’annuaire'));
              }
            });
          },
        );
      });
    });
  }
}
