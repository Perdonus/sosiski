from app.handlers.core import router as core_router
from app.handlers.db_admin import router as db_admin_router
from app.handlers.support import router as support_router
from app.handlers.broadcast import router as broadcast_router
from app.handlers.admin import router as admin_router
from app.handlers.giveaway_admin import router as giveaway_admin_router
from app.handlers.donate import router as donate_router
from app.handlers.giveaway import router as giveaway_router
from app.handlers.referrals import router as referrals_router
from app.handlers.rolls import router as rolls_router
from app.handlers.sausages import router as sausages_router
from app.handlers.top import router as top_router
from app.handlers.trades import router as trades_router
from app.handlers.webapp import router as webapp_router
from app.handlers.showcase import router as showcase_router

routers = [
    core_router,
    db_admin_router,
    support_router,
    admin_router,
    giveaway_admin_router,
    broadcast_router,
    rolls_router,
    sausages_router,
    donate_router,
    trades_router,
    top_router,
    referrals_router,
    giveaway_router,
    webapp_router,
    showcase_router,
]
