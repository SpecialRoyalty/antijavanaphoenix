# Telegram Promo Guard Bot — Railway + PostgreSQL

Bot Telegram de modération/promo pour un groupe, prêt à déployer sur Railway.

## Fonctions

- Supprime les messages d’entrée/sortie.
- Panel admin en boutons avec `/admin`.
- ON/OFF pour autoriser ou bloquer les messages des utilisateurs.
- Les admins peuvent toujours envoyer des messages.
- Supprime tous les médias : photo, vidéo, document, sticker, vocal, etc.
- Si un nouveau membre envoie un média dans les 2 minutes après son arrivée : mute 1 jour.
- Liste de mots interdits ajoutable/supprimable/modifiable via panel admin.
- Détection insensible aux majuscules et accents : `Pv`, `pV`, `privé`, etc.
- Liens et @ interdits pour les utilisateurs : mute 1 jour, puis 7 jours si récidive.
- Broadcast dans le groupe via panel admin.
- Message aléatoire dissuasif toutes les 2h, supprimé après 2 minutes.
- Système récompense : l’utilisateur clique “Je partage”, reçoit son lien d’invitation personnalisé, et reçoit le lien Gofile en privé après 6 arrivées via ce lien.

## Installation locale

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py
```

## Variables Railway

Créer ces variables dans Railway :

```env
BOT_TOKEN=token_du_bot
DATABASE_URL=postgresql://...
GROUP_ID=-100xxxxxxxxxx
ADMIN_IDS=123456789,987654321
REWARD_IMAGE_URL=https://votre-image.jpg
REWARD_REQUIRED_JOINS=6
```

## Déploiement Railway

1. Crée un bot avec `@BotFather` et récupère le token.
2. Dans `@BotFather`, désactive la privacy du bot : `Bot Settings > Group Privacy > Turn off`.
3. Ajoute le bot dans le groupe.
4. Mets le bot administrateur avec droits : supprimer messages, restreindre membres, inviter utilisateurs.
5. Crée un projet Railway.
6. Ajoute un service PostgreSQL.
7. Ajoute les variables d’environnement.
8. Déploie le repo.
9. Dans Telegram, envoie `/admin`.

## Notes importantes

- Le bot utilise le long polling : pas besoin de webhook.
- Le bot doit être admin du groupe, sinon Telegram refusera les suppressions, mutes et liens d’invitation.
- Pour que le bot puisse envoyer le lien Gofile en privé, l’utilisateur doit avoir déjà lancé le bot avec `/start`.
